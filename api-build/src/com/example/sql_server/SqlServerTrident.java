package com.example.sql_server;

//import static com.example.integrations.sql_server.Operation.*;
//import static com.example.integrations.sql_server.SqlServerChangeType.CHANGE_DATA_CAPTURE;
//import static com.example.integrations.sql_server.SqlServerChangeType.CHANGE_TRACKING;
//import static com.example.integrations.sql_server.SqlServerIncrementalUpdater.max;

import com.example.core.CompleteWithTask;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.sql_server.exceptions.ChangeTrackingException;
import com.example.utils.ExampleClock;
import com.google.common.annotations.VisibleForTesting;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.example.sql_server.Operation.*;
import static com.example.sql_server.SqlServerChangeType.CHANGE_DATA_CAPTURE;
import static com.example.sql_server.SqlServerChangeType.CHANGE_TRACKING;

public class SqlServerTrident extends SqlServerSyncer {
    public static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private final OrderedBucketStorage<ChangeEntry> storage;

    private Future<?> future = null;
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);

    public static final Duration IS_FULL_CHECK_INTERVAL = Duration.ofSeconds(30);
    public static final Duration MAX_IS_FULL_WAIT_DURATION = Duration.ofHours(6);
    public static final Duration PRODUCER_RUN_DELAY = Duration.ofMinutes(30);

    private final Duration isFullCheckDelay;

    private final Set<TableRef> tables;
    private final ConcurrentHashMap<TableRef, Instant> tablesToResync;

    public SqlServerTrident(
            SqlServerSource source,
            SqlServerState state,
            SqlServerInformer informer,
            OrderedBucketStorage<ChangeEntry> tridentStorage,
            Output<SqlServerState> out) {
        super(source, state, informer, out);

        this.tables = informer.includedTables();
        this.storage = tridentStorage;
        this.tablesToResync = new ConcurrentHashMap<>();
        this.isFullCheckDelay = IS_FULL_CHECK_INTERVAL;
    }

    /* Constructor for testing */
    SqlServerTrident(
            SqlServerSource source,
            SqlServerState state,
            SqlServerInformer informer,
            Output<SqlServerState> out,
            OrderedBucketStorage<ChangeEntry> storage,
            Duration isFullCheckInterval) {
        super(source, state, informer, out);

        this.tables = informer.includedTables();
        this.storage = storage;
        tablesToResync = new ConcurrentHashMap<>();
        this.isFullCheckDelay = isFullCheckInterval;
    }

    public boolean isProducerEnabled() {
        return tables.stream().anyMatch(table -> !state.isImportFinished(table));
    }

    /**
     * Starts the trident thread which reads change logs and write them into trident storage.
     */
    public void startProducer() {
        startProducer(() -> source.executeWithRetry(this::produce));
    }

    /**
     * Starts the trident thread with defined runnable (the runnable is custom for tests).
     */
    void startProducer(Runnable runnable) {
        if (future != null) throw new IllegalStateException("Producer thread has already been started");

        syncInProgress.set(true);
        LOG.info("Starting SQL Server Trident Producer Thread");

        ExecutorService service = Executors.newSingleThreadExecutor();
        future = service.submit(() -> runProducerLoop(runnable));
    }

    /**
     * Remove any old trident storage up to the current read cursors.
     */
    public void removeOldTridentStorage() {
        // Delete read cursors
        storage.delete(state.getReadCursors());

        // clear read cursors, they'll get filled again if we consume more items from Trident during the sync
        state.clearReadCursors();
    }

    public boolean hasMore() {
        return storage.hasMore();
    }

    /**
     * Stops the trident producer thread and wait for its completion
     */
    public void stopProducer() {
        LOG.info("Spinning down Trident producer thread");
        syncInProgress.set(false);
        future.cancel(true);

        try {
            do {
                checkProducerIsHealthy();
            } while (!future.isDone());
        } finally {
            future = null;
        }
    }

    @VisibleForTesting
    void checkProducerIsHealthy() {
        if (!isProducerEnabled()) return;

        try {
            future.get(2, TimeUnit.SECONDS);
            // If we get here, it means producer thread is done
            if (syncInProgress.get()) {
                throw new RuntimeException("Trident producer thread has stopped unexpectedly");
            }
        } catch (TimeoutException e) {
            LOG.info("Trident producer thread is still running");
        } catch (CancellationException e) {
            if (syncInProgress.get()) {
                throw new RuntimeException("Trident producer thread has stopped unexpectedly");
            }
            LOG.info("Trident producer thread was blocked/waiting and got cancelled");
        } catch (ExecutionException e) {
            LOG.severe(String.format("Trident producer thread exited with an exception: %s", e.getMessage()));
            Throwable t = e.getCause();

            if (t instanceof CompleteWithTask) {
//                throw (CompleteWithTask) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void closeStorage() {
        storage.close();
    }

    public void read() {
        resetTablesForResync("The snapshot/LSN cursor is invalid, resetting table for resync.");

        if (storage.hasMore()) {
            Map<TableRef, TridentCursor> currentCursors = state.getReadCursors();

            try (TridentIterator<ChangeEntry> iterator = storage.iterator(tables, currentCursors)) {
                Set<TableRef> importedTables =
                        tables.stream()
                                .filter(t -> state.isImportStarted(t) || state.isImportFinished(t))
                                .collect(Collectors.toSet());

                while (iterator.hasNext()) {
                    Map.Entry<TableRef, ChangeEntry> mapEntry = iterator.next();
                    TableRef table = mapEntry.getKey();

                    if (FlagName.RunIncrementalSyncOnlyOnImportedTables.check() && !importedTables.contains(table)) {
                        continue;
                    }

                    SqlServerTableInfo tableInfo = informer.tableInfo(table);
                    ChangeEntry change = mapEntry.getValue();

                    boolean isCTorCDCDelete =
                            change.op == CT_DELETE || change.op == CDC_DELETE || change.op == OLD_UPDATE;

                    Instant opTime = change.opTime;
                    if (!state.isImportFinished(tableInfo.sourceTable)) {
                        opTime = state.getImportBeginTime(tableInfo.sourceTable);
                    }

                    // history mode
                    if (tableInfo.getSyncMode() == SyncMode.History) {
                        if (FlagName.SQLServerHistoryModeCorrection.check()) {
                            if (isCTorCDCDelete)
                                out.delete(tableInfo.tableDefinition(), change.values, getHistoryModeOpTime(change, tableInfo));
                            else out.upsert(tableInfo.tableDefinition(), change.values, getHistoryModeOpTime(change, tableInfo));
                        } else {
                            if (isCTorCDCDelete) out.delete(tableInfo.tableDefinition(), change.values, change.opTime);
                            else out.upsert(tableInfo.tableDefinition(), change.values, change.opTime);
                        }
                    }
                    // Live mode - deletes
                    else if (tableInfo.getSyncMode() == SyncMode.Live && isCTorCDCDelete) {
                        out.delete(tableInfo.tableDefinition(), change.values);
                    }
                    // Legacy mode - delete
                    else if (tableInfo.getSyncMode() == SyncMode.Legacy && change.op == CT_DELETE) {
                        out.update(tableInfo.tableDefinition(), change.values);
                    } else {
                        out.upsert(tableInfo.tableDefinition(), change.values);
                    }

                    updateState(tableInfo, change);
                }

                Map<TableRef, TridentCursor> updatedCursors = iterator.getCursors(currentCursors);
                state.setReadCursors(updatedCursors);

                out.checkpoint(state);
            } catch (InvalidCursorException e) {
                // Mark all tables to be resynced
                tables.forEach(t -> tablesToResync.putIfAbsent(t, ExampleClock.Instant.now()));
                resetTablesForResync("Trident cursor(s) are invalid, resetting table for resync");
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Reading from Trident has failed", e);
            }
        } else {
            LOG.info("Trident storage is empty");
        }

        checkProducerIsHealthy();
    }

    private Instant getHistoryModeOpTime(ChangeEntry change, SqlServerTableInfo tableInfo) {
        // import has not finished
        if (!state.isImportFinished(tableInfo.sourceTable)) {
            return state.getImportBeginTime(tableInfo.sourceTable);
        }
        // import has finished
        return max(change.opTime, state.getImportBeginTime(tableInfo.sourceTable));
    }

    /**
     * Return the max Instant (most recent date/time). If i2 is null, then i1 is returned by default.
     *
     * @param i1 non null timestamp
     * @param i2 optionally null timestamp
     * @return
     */
    static Instant max(Instant i1, Instant i2) {
        if (i1 == null) return i2;
        if (i2 == null) return i1;
        if (i1.compareTo(i2) < 0) {
            return i2;
        }

        return i1;
    }

    private void produce(Connection c) throws Exception {
        List<TableRef> ctTables = informer.filterForChangeType(tables, CHANGE_TRACKING);
        List<TableRef> cdcTables = informer.filterForChangeType(tables, CHANGE_DATA_CAPTURE);
        Long globalSnapshot = null;
        BigInteger maxLsn = null;

        if (FlagName.SqlServerAggregateMinVersionQuery.check()) informer.updateMinChangeVersions();

        for (TableRef table : ctTables) {
            // Initialize globalSnapshot only if there are CT tables to sync
            if (globalSnapshot == null) globalSnapshot = getGlobalSnapshot(c);

            SqlServerTableInfo tableInfo = informer.tableInfo(table);
            Long tridentSnapshot =
                    storage.latestItem(table)
//                            .map(change -> change.changeVersion)
                            .map(change -> 1L)
                            .orElse(getOrInitSnapshot(table, globalSnapshot));

            if (SqlServerIncrementalUpdater.isSnapshotValid(c, tableInfo, tridentSnapshot)) {
                produceCTChanges(c, tableInfo, tridentSnapshot);
            } else {
                // Set table to be resynced if trident snapshot is invalid
                tablesToResync.put(table, ExampleClock.Instant.now());
            }
        }

        ZoneOffset serverTimezone =
                FlagName.SqlServerHistoryModeUtcOpTime.check()
                        ? SqlServerIncrementalUpdater.getServerTimezone(c)
                        : ZoneOffset.ofHours(0);

        for (TableRef table : cdcTables) {
            // Only initialize maxLsn if there are CDC tables to sync
            if (maxLsn == null) maxLsn = SqlServerIncrementalUpdater.getGlobalMaxLSN(c);

            SqlServerTableInfo tableInfo = informer.tableInfo(table);
            BigInteger tridentLsn =
                    storage.latestItem(table).map(change -> change.lsn).orElse(getOrInitLsn(table, maxLsn));

            if (SqlServerIncrementalUpdater.isLsnValid(c, tableInfo.cdcCaptureInstance, tridentLsn)) {
                produceCDCChanges(c, tableInfo, tridentLsn, maxLsn, serverTimezone);
            } else {
                // Set table to be resynced if trident LSN is invalid
                tablesToResync.put(table, ExampleClock.Instant.now());
            }
        }
    }

    private void produceCTChanges(Connection connection, SqlServerTableInfo tableInfo, Long snapshot)
            throws SQLException {
        boolean historySync = tableInfo.getSyncMode() == SyncMode.History;
        String query = SqlServerIncrementalUpdater.ctCatchUpQuery(tableInfo, snapshot);

        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rows = ps.executeQuery()) {
            SqlServerSyncer.SourceValueFetcher<SQLException> typeCoercer = sourceValueFetcher(rows);

            while (rows.next()) {
                String opString = rows.getString("SYS_CHANGE_OPERATION");
                long changeVersion = rows.getLong("SYS_CHANGE_VERSION");
                Instant opTime =
                        historySync ? rows.getTimestamp(SqlServerIncrementalUpdater.OP_TIME).toInstant() : null;

                Operation op = Operation.fromCT(opString);

                Map<String, Object> values;
                if (op == CT_DELETE) {
                    values = extractRowValues(tableInfo, tableInfo.primaryKeys(), typeCoercer, true);
                } else {
                    values = extractRowValues(tableInfo, typeCoercer, false);
                }

                storage.add(tableInfo.sourceTable, ChangeEntry.v1(op, opTime, changeVersion, null, values));
            }
        }
    }

    private Long getOrInitSnapshot(TableRef table, Long globalSnapshot) {
        return state.getSnapshot(table)
                .orElseGet(
                        () -> {
                            // Initialize CT snapshot for new table
                            state.updateSnapshotV3(table, Optional.of(globalSnapshot));
                            return globalSnapshot;
                        });
    }

    private Long getGlobalSnapshot(Connection connection) throws ChangeTrackingException {
        return SqlServerIncrementalUpdater.getGlobalSnapshot(connection, source.credentials.database.get());
    }

    private void produceCDCChanges(
            Connection connection,
            SqlServerTableInfo tableInfo,
            BigInteger currentLsn,
            BigInteger maxLsn,
            ZoneOffset serverTimezone)
            throws SQLException {
        boolean historySync = tableInfo.getSyncMode() == SyncMode.History;
        String query = SqlServerIncrementalUpdater.cdcCatchUpQuery(tableInfo, currentLsn, maxLsn, historySync);

        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rows = ps.executeQuery()) {
            SqlServerSyncer.SourceValueFetcher<SQLException> typeCoercer = sourceValueFetcher(rows);

            while (rows.next()) {
                Instant opTime =
                        historySync
                                ? ZonedDateTime.of(rows.getTimestamp("op_time").toLocalDateTime(), serverTimezone)
                                .toInstant()
                                : null;
                int opInt = rows.getInt("__$operation");
                byte[] rawLsn = rows.getBytes("__$start_lsn");

                Operation op = Operation.fromCDC(opInt);
                BigInteger lsn = new BigInteger(1, rawLsn);

                boolean deleted = op == CDC_DELETE || op == OLD_UPDATE;

                Map<String, Object> values = extractRowValues(tableInfo, typeCoercer, deleted);

                storage.add(tableInfo.sourceTable, ChangeEntry.v1(op, opTime, null, lsn, values));
            }
        }
    }

    private BigInteger getOrInitLsn(TableRef table, BigInteger maxLsn) {
        return state.getLsn(table)
                .orElseGet(
                        () -> {
                            // Initialize CDC LSN for new table
                            state.updateLsn(table, Optional.of(maxLsn));
                            return maxLsn;
                        });
    }

    private void updateState(SqlServerTableInfo tableinfo, ChangeEntry change) {
        if (tableinfo.changeType == CHANGE_TRACKING) {
            state.updateSnapshotV3(tableinfo.sourceTable, Optional.of(change.changeVersion));

        } else if (tableinfo.changeType == CHANGE_DATA_CAPTURE) {
            state.updateLsn(tableinfo.sourceTable, Optional.of(change.lsn));

        } else {
            throw new IllegalArgumentException("Cannot update state for untracked table");
        }
    }

    /**
     * Reset states & cursors, delete trident data for all resync tables
     */
    private void resetTablesForResync(String messageForTable) {
        for (TableRef table : tablesToResync.keySet()) {
            resyncWarning(table, messageForTable);

            // Remove changes for table within trident storage
            storage.remove(table);

            // Reset read cursor for table
            state.clearReadCursorFor(table);

            // Re-initialize table for re-sync
            state.resetTableState(table);
        }

        // Since we reset all tables, we can clear the table map
        tablesToResync.clear();
    }

    private void runProducerLoop(Runnable runnable) {
        TridentMetrics tridentMetrics = new TridentContext().getTridentMetrics();
        TridentMetrics.TridentTimer blockedTimer = tridentMetrics.getBlockedProducerTimer();

        LOG.info("Trident producer thread is running");
        Instant lastRun = Instant.ofEpochMilli(0);

        do {
            try {
                if (storage.isFull()) {
                    LOG.info("Trident is still full");

                    if (maybeRescheduleSync(lastRun)) {
//                        throw Rescheduled.retryAfter(
//                                Duration.ZERO, "Trident storage is still full, rescheduling new sync");
                    } else {
                        // wait a bit and check again
                        waitForMillis(isFullCheckDelay.toMillis());
                        continue;
                    }
                } else {
                    blockedTimer.stop();
                }

                Optional<Long> delayMillis = maybeDelayNextRun(lastRun);
                if (delayMillis.isPresent()) {
                    waitForMillis(delayMillis.get());
                }

                LOG.info("Producer thread fetching incremental changes");
                runnable.run();
                lastRun = ExampleClock.Instant.now();

//            } catch (MaxDiskUsageException e) {
            }
//            catch (Exception e) {
//                LOG.warning("Trident max local disk usage reached");
//                lastRun = exampleClock.Instant.now();
//                blockedTimer.start();
//
//            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warning("Trident producer thread got interrupted");
                return;
            }
        } while (syncInProgress.get());

        LOG.info("Trident producer thread completed successfully");
    }

    boolean maybeRescheduleSync(Instant lastRun) {
        Instant now = ExampleClock.Instant.now();

        return now.isAfter(lastRun.plus(MAX_IS_FULL_WAIT_DURATION));
    }

    Optional<Long> maybeDelayNextRun(Instant lastRun) {
        Instant now = ExampleClock.Instant.now();

        // If PRODUCER_RUN_DELAY amount of time has passed since last run, do not delay
        if (ExampleClock.Instant.now().isAfter(lastRun.plus(PRODUCER_RUN_DELAY))) {
            return Optional.empty();
        }

        return Optional.of(PRODUCER_RUN_DELAY.minus(Duration.between(lastRun, now)).toMillis());
    }

    private static void waitForMillis(long millis) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(millis);
    }

    public void delete(Map<TableRef, TridentCursor> readCursors) {

    }
}
