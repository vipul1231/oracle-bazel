package com.example.sql_server;

import com.example.core.SyncMode;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbIncrementalUpdater;
import com.example.db.DbRowSize;
import com.example.db.SqlStatementUtils;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.micrometer.TagValue;
import com.example.sql_server.exceptions.ChangeDataCaptureException;
import com.example.sql_server.exceptions.ChangeTrackingException;
import com.example.sql_server.exceptions.SqlServerAgentOccupied;
import io.micrometer.core.instrument.LongTaskTimer;
import io.opentelemetry.extension.annotations.WithSpan;

import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.example.sql_server.SqlServerIncrementalUpdater.CDCOperation.DELETE;
import static com.example.sql_server.SqlServerIncrementalUpdater.CDCOperation.OLD_UPDATE;

public class SqlServerIncrementalUpdater extends SqlServerSyncer implements DbIncrementalUpdater {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    public static final String OP_TIME = "op_time";
    private Duration extractUpdateDuration = Duration.ofSeconds(0);
    //    LongTaskTimer updateTimer =
//            exampleConnectorMetrics.get()
//                    .longTaskTimer(MetricDefinition.SYNC_DURATION, TagName.SYNC_PHASE.toString(), "incremental_update");
    LongTaskTimer updateTimer = null;

    private long ctInserts, ctUpdates, ctDeletes = 0L;

    public SqlServerIncrementalUpdater(
            SqlServerSource sqlServerSource,
            SqlServerState sqlServerState,
            SqlServerInformer sqlServerInformer,
            Output<SqlServerState> out) {
        super(sqlServerSource, sqlServerState, sqlServerInformer, out);
    }

    public static ZoneOffset getServerTimezone(Connection c) {
        return ZoneOffset.ofHours(0);
    }


    /**
     * Abstraction for synchronization strategies
     */
    public abstract class DataSynchronizationStrategy {

        public SqlServerTableInfo tableInfo;
        SourceValueFetcher<SQLException> typeCoercer;
        public ResultSet resultSet;

        /**
         * @param tableInfo
         * @param typeCoercer
         * @param resultSet
         */
        DataSynchronizationStrategy(
                SqlServerTableInfo tableInfo, SourceValueFetcher<SQLException> typeCoercer, ResultSet resultSet) {
            this.tableInfo = tableInfo;
            this.typeCoercer = typeCoercer;
            this.resultSet = resultSet;
        }

        abstract void sync(CDCOperation op) throws SQLException;
    }

    /**
     * Implements the history mode strategy
     */
    public class HistoryModeSyncStrategy extends DataSynchronizationStrategy {

        /**
         * @param tableInfo
         * @param typeCoercer
         * @param resultSet
         */
        HistoryModeSyncStrategy(
                SqlServerTableInfo tableInfo, SourceValueFetcher<SQLException> typeCoercer, ResultSet resultSet,
                ZoneOffset serverTimezone) {
            super(tableInfo, typeCoercer, resultSet);
        }

        @Override
        void sync(CDCOperation op) throws SQLException {
            Instant opTime = resultSet.getTimestamp(OP_TIME).toInstant();

            // reset operation time to import begin time, when import has NOT been finished
            // this will be applicable only for state.isCheckWithImportStartTime()
            if (!state.isImportFinished(tableInfo.sourceTable)) {
                LOG.info("operationTime" + opTime + " Table import is not finished. Resetting update time");
                opTime = state.getImportBeginTime(tableInfo.sourceTable);
            }
            
            boolean deleted = op == DELETE || op == OLD_UPDATE;

            Map<String, Object> rowValues =
                    extractRowValues(tableInfo, tableInfo.includedColumnInfo(), typeCoercer, deleted);

            if (deleted) deleteRecord(tableInfo.tableDefinition(), rowValues, opTime);
            else upsertRecord(tableInfo.tableDefinition(), rowValues, opTime);
        }
    }

    public void updateRecord(TableDefinition table, Map<String, Object> rowValues) {
        out.update(table, rowValues);
        processDataVol(rowValues);
    }

    public void upsertRecord(TableDefinition table, Map<String, Object> rowValues) {
        out.upsert(table, rowValues);
        processDataVol(rowValues);
    }

    public void upsertRecord(TableDefinition table, Map<String, Object> rowValues, Instant opTime) {
        out.upsert(table, rowValues, opTime);
        processDataVol(rowValues);
    }

    public void deleteRecord(TableDefinition table, Map<String, Object> rowValues, Instant opTime) {
        out.delete(table, rowValues, opTime);
        processDataVol(rowValues);
    }

    private void processDataVol(Map<String, Object> rowValues) {
        long rowSize = DbRowSize.mapValues(rowValues);
//        byteCounter.increment(DbRowSize.mapValues(rowValues));
//        rowCounter.increment();
        extractVol += rowSize;
    }

    /**
     * Implements the non-history mode strategy
     */
    private class NonHistoryModeSyncStrategy extends DataSynchronizationStrategy {

        /**
         * @param tableInfo
         * @param typeCoercer
         */
        NonHistoryModeSyncStrategy(SqlServerTableInfo tableInfo, SourceValueFetcher<SQLException> typeCoercer) {
            super(tableInfo, typeCoercer, null);
        }

        @Override
        void sync(CDCOperation op) throws SQLException {
            upsertRecord(
                    tableInfo.tableDefinition(),
                    extractRowValues(tableInfo, typeCoercer, op == DELETE || op == OLD_UPDATE));
        }
    }

    @Override
    @WithSpan
    public void incrementalUpdate(Set<TableRef> includedTables) {
//        LongTaskTimer.Sample updateTimerSample = updateTimer.start();
        incrementalUpdateCTTables(informer.filterForChangeType(includedTables, SqlServerChangeType.CHANGE_TRACKING));
        incrementalUpdateCDCTables(informer.filterForChangeType(includedTables, SqlServerChangeType.CHANGE_DATA_CAPTURE));
//        updateTimerSample.stop();
    }

    @WithSpan
    public void incrementalUpdateCTTables(List<TableRef> ctTables) {
        if (ctTables.isEmpty()) return;

        Long snapshot =
                source.executeWithRetry(
                        connection -> {
                            if (ctTables.stream().anyMatch(t -> informer.syncModes().get(t) != SyncMode.Legacy))
                                return getGlobalMaxCommitVersionNum(connection);
                            else {
                                String database =
                                        source.credentials.database.orElseThrow(
                                                () -> new IllegalStateException("Must have a database in SQL Server"));
                                return getGlobalSnapshot(connection, database);
                            }
                        });

        if (FlagName.SqlServerAggregateMinVersionQuery.check()) informer.updateMinChangeVersions();

        Optional<Long> maybeSnapshot = Optional.ofNullable(snapshot);
        for (TableRef table : ctTables) {
            source.executeWithRetry(
                    connection -> {
                        snapshot(maybeSnapshot, table, connection);
                    });

            out.checkpoint(state);
        }
    }

    private void snapshot(Optional<Long> currentSnapshot, TableRef table, Connection connection)
            throws ChangeTrackingException, SQLException {
        Optional<Long> tableSnapshot = state.getSnapshot(table);

        if (tableSnapshot.isPresent()) {
            if (!isSnapshotValid(connection, informer.tableInfo(table), tableSnapshot.get())) {
                resyncWarning(
                        table,
                        "Snapshot is currently invalid, it has been too long since the last incremental update.");

                // Re-initialize table with invalid snapshots for re-sync
                state.resetTableState(table);

                // Skip the incremental update for this table
                return;
            }
        } else {
            // If table is un-imported, then set table cursor to latest snapshot to minimize updates
            tableSnapshot = currentSnapshot;
        }

        ctCatchUp(
                connection,
                table,
                tableSnapshot.orElseThrow(() -> new IllegalStateException("tableSnapshot should never be null.")));

        state.updateSnapshotV3(table, currentSnapshot);
    }

    /**
     * See https://github.com/example/engineering/issues/72949
     *
     * @param connection
     * @param table
     * @param tableSnapshot
     * @throws SQLException
     */
    @WithSpan
    public void ctCatchUp(Connection connection, TableRef table, Long tableSnapshot) throws SQLException {
        SqlServerTableInfo tableInfo = informer.tableInfo(table);
        SyncMode syncMode = informer.syncModes().get(table);
        String query = ctCatchUpQuery(tableInfo, tableSnapshot);

        if (state.getImportBeginTime(table) == null) {
            System.out.println("Importer for this table hasn't been started yet for - " + table);
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rows = ps.executeQuery()) {
            SourceValueFetcher<SQLException> typeCoercer = sourceValueFetcher(rows);

            long rowsWritten = 0;
            long bytesOfDataBeforePage = this.extractVol;

            Instant opTime = null;
            while (rows.next()) {
                String op = rows.getString("SYS_CHANGE_OPERATION");

                if (isIllegalOperation(op)) {
                    throw new IllegalArgumentException(
                            "Change tracking operation must be of the following characters: 'U', 'I', 'D'. Actual value is: "
                                    + op);
                }

                recordChanges(op);

                // We cannot upsert deletes from Change Tracking since non-primary key columns are not captured.
                // Instead we update the _example_deleted column using the primary keys of the row.
                boolean isDelete = op.equals("D");

                if (syncMode == SyncMode.History) {
                    opTime = rows.getTimestamp(OP_TIME).toInstant();
                    if (opTime.isBefore(state.getImportBeginTime(table))) {
                        System.out.println(table + " - operationTime " + opTime + " This operation happens before the importer started. so ignore it for this iteration");
                        continue;
                    }

                    // reset operation time to import begin time, when import has NOT been finished
                    // this will be applicable only for state.isCheckWithImportStartTime()
                    if (!state.isImportFinished(tableInfo.sourceTable)) {
                        LOG.info("operationTime" + opTime + " Table import is not finished. Resetting update time");
                        opTime = state.getImportBeginTime(tableInfo.sourceTable);
                    }

                    if (isDelete) {
                        deleteRecord(
                                tableInfo.tableDefinition(),
                                extractRowValues(tableInfo, tableInfo.primaryKeys(), typeCoercer, true),
                                opTime);
                    } else
                        upsertRecord(
                                tableInfo.tableDefinition(), extractRowValues(tableInfo, typeCoercer, false), opTime);
                } else {
                    if (isDelete) {
                        updateRecord(
                                tableInfo.tableDefinition(),
                                extractRowValues(tableInfo, tableInfo.primaryKeys(), typeCoercer, true));
                    } else upsertRecord(tableInfo.tableDefinition(), extractRowValues(tableInfo, typeCoercer, false));
                }

                rowsWritten++;
            }
            logExtractVol(table.toString(), (this.extractVol - bytesOfDataBeforePage));
            if (rowsWritten > 0) {
//                LOG.notice(ProcessedRecordsEvent.of(table.toString(), rowsWritten));
            } else {
                LOG.notice(table + ": has no rows to update incrementally");
            }
        }
        logChanges();
    }

    public static String ctCatchUpQuery(SqlServerTableInfo tableInfo, Long tableSnapshot) {
        SyncMode syncMode = tableInfo.getSyncMode();
        Collection<String> keyCols = new ArrayList<>();
        Collection<String> otherCols = new ArrayList<>();
        Collection<String> clausesToJoin = new ArrayList<>();

        tableInfo
                .sourceColumnInfo()
                .forEach(
                        col -> {
                            if (col.isPrimaryKey) {
                                keyCols.add("CT." + wrap(col));
                                clausesToJoin.add(
                                        "CT."
                                                + SqlStatementUtils.SQL_SERVER.quote(col.columnName)
                                                + " = "
                                                + "TB."
                                                + SqlStatementUtils.SQL_SERVER.quote(col.columnName));
                            } else {
                                otherCols.add("TB." + wrap(col));
                            }
                        });

        if (syncMode == SyncMode.History) {
            otherCols.add("TIMETABLE.COMMIT_TIME as op_time");
        }

        String keyColumns = String.join(", ", keyCols);
        String otherColumns = otherCols.isEmpty() ? "" : String.join(", ", otherCols) + ", ";
        String joinClause = String.join(" AND ", clausesToJoin);
        String historyModeClause =
                " JOIN sys.dm_tran_commit_table TIMETABLE on CT.sys_change_version = TIMETABLE.commit_ts";

        String query =
                "SELECT "
                        + keyColumns
                        + ", "
                        + otherColumns
                        + "CT.[SYS_CHANGE_OPERATION], CT.[SYS_CHANGE_VERSION] FROM "
                        + SqlStatementUtils.SQL_SERVER.quote(tableInfo.sourceTable)
                        + " AS TB "
                        + "RIGHT JOIN (SELECT * FROM CHANGETABLE(CHANGES "
                        + SqlStatementUtils.SQL_SERVER.quote(tableInfo.sourceTable)
                        + ", "
                        + tableSnapshot.toString()
                        + ") ET) CT ON "
                        + joinClause;
        if (syncMode == SyncMode.History) query += historyModeClause;

        return query;
    }

    private boolean isIllegalOperation(String op) {
        return !op.equals("U") && !op.equals("I") && !op.equals("D");
    }

    private void logChanges() {
        LOG.notice(
                String.format(
                        "Changes so far this sync:\n" + "Inserts: %d\n" + "Updates: %d\n" + "Deletes: %d",
                        ctInserts, ctUpdates, ctDeletes));
    }

    private void recordChanges(String op) {
        switch (op) {
            case "I":
                ctInserts++;
                break;
            case "U":
                ctUpdates++;
                break;
            case "D":
                ctDeletes++;
                break;
        }
    }

    @WithSpan
    private void incrementalUpdateCDCTables(List<TableRef> cdcTables) {
        // for any table get the latest lsn
        Optional<BigInteger> latestLsn =
                cdcTables.isEmpty()
                        ? Optional.empty()
                        : Optional.of(source.executeWithRetry(SqlServerIncrementalUpdater::getGlobalMaxLSN));
//        Optional<BigInteger> latestLsn = Optional.empty();
        System.out.println("############### latestLsn :" + toPaddedHex(latestLsn.get()) + " cdcTables.size :" + cdcTables.size());

        for (TableRef table : cdcTables) {
            source.executeWithRetry(
                    connection -> {
                        Optional<BigInteger> tableLsn = state.getLsn(table);
                        String captureInstance = informer.tableInfo(table).cdcCaptureInstance;

                        if (tableLsn.isPresent()) {
                            if (!isLsnValid(connection, captureInstance, tableLsn.get())) {
                                resyncWarning(
                                        table,
                                        "LSN is currently invalid, it has been too long since the last incremental update");

                                // Re-initialize table with invalid snapshots for re-sync
                                state.resetTableState(table);

                                // Skip the incremental update for this table
                                return;
                            }
                        } else {
                            // We are treating empty LSNs as valid, since the table has yet to be imported.
                            // If table is un-imported, then set table cursor to latest LSN to minimize updates
                            tableLsn = latestLsn;
                            if (FeatureFlag.check("SqlServerIdenticalLSN")) {
                                state.updateLsn(table, latestLsn);
                                return;
                            }
                        }
                        if (FeatureFlag.check("SqlServerIdenticalLSN")) {
                            if (!(tableLsn.get()).equals(latestLsn.get())) {
                                cdcCatchUp(connection, table, tableLsn.get(), latestLsn.get(), OffsetDateTime.now(ZoneId.systemDefault()).getOffset());
                                state.updateLsn(table, latestLsn);
                            }
                        } else {
                            cdcCatchUp(connection, table, tableLsn.get(), latestLsn.get(), OffsetDateTime.now(ZoneId.systemDefault()).getOffset());
//                            cdcCatchUp(connection, table, BigInteger.ONE, BigInteger.ONE);
                            state.updateLsn(table, latestLsn);
                        }
                    });
            out.checkpoint(state);
        }
    }


    public DataSynchronizationStrategy getSyncStrategy(Boolean historySync, SqlServerTableInfo tableInfo,
                                                       SourceValueFetcher<SQLException> typeCoercer, ResultSet rows, ZoneOffset serverTimezone) {
        return (historySync
                ? new HistoryModeSyncStrategy(tableInfo, typeCoercer, rows, serverTimezone)
                : new NonHistoryModeSyncStrategy(tableInfo, typeCoercer));
    }

    @WithSpan
    public void cdcCatchUp(Connection connection, TableRef table, BigInteger currentLsn, BigInteger maxLsn, ZoneOffset serverTimezone)
            throws SQLException {
        System.out.println("----------- Calling cdcCatchUp --------------");
        SqlServerTableInfo tableInfo = informer.tableInfo(table);
//        boolean historySync = informer.syncModes().get(table) == SyncMode.History;
        boolean historySync = true;

        if (state.getImportBeginTime(table) == null) {
            System.out.println("Importer for this table hasn't been started yet for - " + table);
            return;
        }
        String query = cdcCatchUpQuery(tableInfo, currentLsn, maxLsn, historySync);
        System.out.println("Last Imported Time for the table " + table.name + " is :" + state.getImportFinishedTime(table));
        System.out.println("Script :" + query);
        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rows = ps.executeQuery()) {
            SourceValueFetcher<SQLException> typeCoercer = sourceValueFetcher(rows);

            long rowsWritten = 0;

            DataSynchronizationStrategy syncStrategy = getSyncStrategy(historySync, tableInfo, typeCoercer, rows, serverTimezone);

            while (rows.next()) {
                System.out.println("--------------- > Rows Found.");
                int opInt = rows.getInt("__$operation");

                Instant operationTime = null;
                if (historySync) {
                    operationTime = rows.getTimestamp(OP_TIME).toInstant();
                    if (operationTime.isBefore(state.getImportBeginTime(table))) {
                        System.out.println(table + " - operationTime " + operationTime + " operation :" + opInt + " This operation happens before the importer started. so ignore it for this iteration");
                        continue;
                    }
                }

                CDCOperation op = CDCOperation.valueOf(opInt);

                syncStrategy.sync(op);

                rowsWritten++;
            }

            if (rowsWritten > 0) {
//                LOG.info(ProcessedRecordsEvent.of(table.toString(), rowsWritten));
            } else {
                LOG.info(table + ": has no rows to update incrementally");
            }
        }
    }

    static String cdcCatchUpQuery(
            SqlServerTableInfo tableInfo, BigInteger currentLsn, BigInteger maxLsn, boolean historySync) {
        String columns =
                tableInfo.includedColumnInfo().stream().map(SqlServerSyncer::wrap).collect(Collectors.joining(", "));

        return "SELECT "
                + (historySync ? "sys.fn_cdc_map_lsn_to_time(__$start_lsn) as op_time, " : "")
                + columns
                + ",__$start_lsn ,__$operation FROM cdc."
                + SqlStatementUtils.SQL_SERVER.quote("fn_cdc_get_all_changes_" + tableInfo.cdcCaptureInstance)
                + "("
                + incrementToNextValidLsn(currentLsn, maxLsn)
                + ", "
                + toPaddedHex(maxLsn)
                + ", N'all update old')";
    }

    public static Long getGlobalSnapshot(Connection connection, String database) throws ChangeTrackingException {
        try (Statement statement = connection.createStatement();
             ResultSet result =
                     statement.executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version")) {
            if (!result.next()) {
                throw new ChangeTrackingException(
                        "Zero rows returned while trying to retrieve change tracking version");
            }

            Long currentSnapshot = (Long) result.getObject("current_version");

            if (currentSnapshot == null) {
                ChangeTrackingException e =
                        new ChangeTrackingException("ChangeTracking is not enabled for database: " + database);
//                throw CompleteWithTask.enableDatabaseChangeTracking(database, e);
            }

            return currentSnapshot;
        } catch (SQLException e) {
            throw new ChangeTrackingException("Unable to retrieve change tracking version", e);
        }
    }

    public static Long getGlobalMaxCommitVersionNum(Connection connection) throws ChangeTrackingException {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "SELECT MAX(commit_ts) as MAX_GLOBAL_COMMIT FROM sys.dm_tran_commit_table");
             ResultSet result = statement.executeQuery()) {

            if (!result.next()) {
                throw new ChangeTrackingException(
                        "Zero rows returned while trying to retrieve change tracking version");
            }

            // if there are no changes, this returns null. this also returns null if CT is disabled, but our code
            // won't let you get here if CT is disabled. so we can assume no changes -> snapshot at 0, the beginning
            Object maxCommitObj = result.getObject("MAX_GLOBAL_COMMIT");

            return maxCommitObj == null ? 0L : (Long) maxCommitObj;
        } catch (SQLException e) {
            throw new ChangeTrackingException("Unable to retrieve change tracking version", e);
        }
    }

    /**
     * @param connection
     * @param table
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("java:S2095")
    private static PreparedStatement prepareSelectChangeTrackingStatement(Connection connection, TableRef table)
            throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?)) AS min_valid_version");
        statement.setString(1, table.toString());

        return statement;
    }

    /**
     * @param connection
     * @param tableInfo
     * @param snapshot
     * @return
     * @throws ChangeTrackingException
     */
    static boolean isSnapshotValid(Connection connection, SqlServerTableInfo tableInfo, long snapshot)
            throws ChangeTrackingException {
        TableRef table = tableInfo.sourceTable;

        if (tableInfo.minChangeVersion == null) {
            LOG.warning("Minimum change version is null for table: " + table);
        } else if (FlagName.SqlServerAggregateMinVersionQuery.check()) {
            return (snapshot >= tableInfo.minChangeVersion - 1);
        }

        try (PreparedStatement statement = prepareSelectChangeTrackingStatement(connection, table);
             ResultSet results = statement.executeQuery();) {
            if (!results.next()) {
                throw new ChangeTrackingException(
                        "Zero rows returned while trying to retrieve minimum change tracking version for table: "
                                + table);
            }

            long minValidVersion = results.getLong("min_valid_version");
            // We allow MinValidVersion-1 because if Change Tracking was just enabled and no updates were recorded
            // to the ChangeTable then MinValidVersion can be 1 more than ChangeTrackingCurrentVersion
            return (snapshot >= minValidVersion - 1);
        } catch (SQLException e) {
            throw new ChangeTrackingException("Unable to validate change tracking version for table: " + table, e);
        }
    }

    public static BigInteger getGlobalMaxLSN(Connection connection) throws ChangeDataCaptureException, SqlServerAgentOccupied {
        try (PreparedStatement statement = connection.prepareStatement("SELECT sys.fn_cdc_get_max_lsn() AS lsn");
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new ChangeDataCaptureException("Zero rows returned while trying to capture max LSN");
            }

            byte[] rawLsn = result.getBytes("lsn");

            // lsn can be null when no activity has been recorded by CDC. Usually this just means the sql agent has not
            // parsed the internal transaction log yet. Waiting a few seconds can solve this.
            if (rawLsn == null) {
                throw new SqlServerAgentOccupied("Current LSN is null. Wait for SQL Agent to catch up.");
            }

            return new BigInteger(1, rawLsn);
        } catch (SQLException e) {
            throw new ChangeDataCaptureException("Unable to retrieve maximum change data capture LSN version", e);
        }
    }

    /**
     *
     */
    @SuppressWarnings("java:S2095")
    private static PreparedStatement prepareLsnStatement(Connection connection, String captureInstance)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT sys.fn_cdc_get_min_lsn(?) AS min_valid_lsn");
        statement.setString(1, captureInstance);

        return statement;
    }

    public static boolean isLsnValid(Connection connection, String captureInstance, BigInteger lsn)
            throws ChangeDataCaptureException, SqlServerAgentOccupied {
        try (PreparedStatement statement = prepareLsnStatement(connection, captureInstance);
             ResultSet result = statement.executeQuery();) {
            if (!result.next()) {
                throw new ChangeDataCaptureException(
                        "Zero rows returned while trying to capture minimum LSN for capture instance: "
                                + captureInstance);
            }

            byte[] rawMinValidLsn = result.getBytes("min_valid_lsn");

            // lsn can be null when no activity has been recorded by CDC. Usually this just means the sql agent has
            // not parsed the internal transaction log yet. Waiting a few seconds can solve this.
            if (rawMinValidLsn == null) {
                throw new SqlServerAgentOccupied("Current LSN is null. Wait for SQL Agent to catch up.");
            }

            BigInteger minValidLsn = new BigInteger(1, rawMinValidLsn);

            return lsn.compareTo(minValidLsn) > 0;
        } catch (SQLException e) {
            throw new ChangeDataCaptureException(
                    "Unable to validate minimum change data capture LSN for capture instance: " + captureInstance, e);
        }
    }

    private static String incrementToNextValidLsn(BigInteger startLsn, BigInteger maxLsn) {
        return maxLsn.compareTo(startLsn) > 0
                ? "sys.fn_cdc_increment_lsn(" + toPaddedHex(startLsn) + ")"
                : toPaddedHex(startLsn);
    }

    /**
     * Returns a hex string padded to 20 hexadecimal digits
     *
     * @param lsn The log sequence number to convert into a the hex string.
     * @return A formatted, 20 hex digit padded string representing the LSN
     * <p>ex. 0x11111111111111 -> 0x00000011111111111111
     */
    private static String toPaddedHex(BigInteger lsn) {
        return String.format("0x%020x", lsn);
    }

    /**
     * TODO: Replace this enum with {@link Operation} CDC operations are represented by numbers ranging from 1 to 4.
     * more info:
     * https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql#table-returned
     */
    enum CDCOperation {
        DELETE,
        INSERT,
        OLD_UPDATE,
        UPDATE;

        static CDCOperation valueOf(int op) {
            switch (op) {
                case 1:
                    return DELETE;
                case 2:
                    return INSERT;
                case 3:
                    return OLD_UPDATE;
                case 4:
                    return UPDATE;
                default:
                    throw new IllegalArgumentException(
                            "Invalid CDC operation is not between 1 and 4, actual value is: " + op);
            }
        }
    }

    public void recordUpdateDuration(TagValue.SyncStatus syncStatus, String dbVersion) {
        if (!(extractUpdateDuration.isZero() || extractUpdateDuration.isNegative())) {
            // using `simpleTimer` since we are not pre calculate percentiles
//            METRICS.simpleTimer(
//                    MetricDefinition.SYNC_EXTRACT_UPDATE_DURATION,
//                    TagName.SYNC_STATUS.toString(),
//                    syncStatus.value,
//                    TagName.DB_VERSION.toString(),
//                    dbVersion)
//                    .record(extractUpdateDuration);
            LOG.info(String.format("Update duration %d ms sent to NewRelic", extractUpdateDuration.toMillis()));
        }
    }
}
