package com.example.oracle;

import com.example.core.Column;
import com.example.core.CompleteWithTask;
import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.lambda.Exceptions;
import com.example.logger.ExampleLogger;
import com.example.logger.LogEvent;
import com.example.logger.event.integration.ImportProgressEvent;
import com.example.logger.event.integration.InfoEvent;
import com.example.logger.event.integration.ReadEvent;
import com.example.logger.event.integration.WarningEvent;
import com.example.logging.progress.DbImportProgress;
import com.example.micrometer.TagValue;
import com.example.oracle.cache.PartitionedTableCache;
import com.example.oracle.cache.TableRowCountCache;
import com.example.oracle.exceptions.ImportFailureException;
import com.example.oracle.exceptions.InvalidScnException;
import com.example.oracle.exceptions.MissingObjectException;
import com.example.oracle.logminer.OracleLogMinerIncrementalUpdater;
import com.example.oracle.spi.OracleAbstractIncrementalUpdater;
import com.example.oracle.warnings.NoTableAccessType;
import com.example.oracle.warnings.NoTableAccessWarning;
import com.example.utils.ExampleClock;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.example.micrometer.TagValue.SyncStatus.*;
import static com.example.oracle.Constants.*;
import static com.example.oracle.SqlUtil.getSelectCols;
import static com.example.oracle.Util.doubleQuote;
import static com.example.oracle.Util.singleQuote;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:43 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleUpdater implements DataUpdater<OracleState> {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    private static final Duration LOG_IMPORT_PROGRESS_EVERY = Duration.ofMinutes(30);
    private static final long PROD_FAST_IMPORT_LIMIT = 1_000_000;
    private static final int ORACLE_SCN_PRECISION = 3; // Oracle SCN is accurate to the time +- 3 seconds

    public final OracleApi api;
    //    public final AbstractHashIdGenerator hashIdGenerator;
    private final OracleService service;
    private final ConnectionParameters params;
    private final OracleResyncHelper resyncHelper;
    private final OracleMetricsHelper metricsHelper;

    private DbaExtendsHandler dbaExtendsHandler;
    private TableRowCountCache tableRowCountCache;
    private OracleLogMinerIncrementalUpdater logMinerIncrementalUpdater;

    OracleOutputHelper outputHelper;
    Output<OracleState> output;
    OracleAbstractIncrementalUpdater incrementalUpdater;
    private DatabaseDao databaseDao;

    private Instant lastImportProgressEvent = Instant.EPOCH;
    private OracleState state;
    private long fastImportLimit = PROD_FAST_IMPORT_LIMIT;
    private static final Duration PROD_GUARANTEED_IMPORT_DURATION = Duration.ofHours(1);
    private Duration guaranteedImportDuration = PROD_GUARANTEED_IMPORT_DURATION;

    private final OracleConnectorContext oracleConnectorContext;
    private final boolean useFlashback;
    private boolean requireRunningIncrementalSync = true;

    OracleUpdater(OracleConnectorContext context) {
        this.service = context.getOracleService();
        this.params = context.getConnectionParameters();
        this.api = context.getOracleApi();

        this.useFlashback = FeatureFlag.check("OracleFlashback");
        this.oracleConnectorContext = context;
        this.resyncHelper = context.getOracleResyncHelper();
        this.metricsHelper = context.getOracleMetricsHelper();
    }

    OracleUpdater(
            OracleService service,
            OracleApi api,
            OracleState state,
            ConnectionParameters params,
//            AbstractHashIdGenerator hashIdGenerator,
            OracleConnectorContext context) {
        this.state = state;
        this.params = params;
        this.api = api;
        this.service = service;
//        this.hashIdGenerator = hashIdGenerator;
        this.oracleConnectorContext = context;
        this.resyncHelper = context.getOracleResyncHelper();
        this.metricsHelper = context.getOracleMetricsHelper();

        // Optional values
        this.outputHelper = context.getOracleOutputHelper();
        this.output = context.getOutput();

        this.useFlashback = FeatureFlag.check("OracleFlashback");

        dbaExtendsHandler = new DbaExtendsHandler();
        tableRowCountCache = new TableRowCountCache();

        databaseDao = new DatabaseDao();
        logMinerIncrementalUpdater = new OracleLogMinerIncrementalUpdater(api);
    }

    /**
     * Fetch all rows in one big query
     */
    @SuppressWarnings("java:S2095")
    private PreparedStatement selectImportRowsAll(Connection connection, TableRef table, List<OracleColumn> columns)
            throws SQLException {
        String from = doubleQuote(table);
        String allCols = getSelectCols(columns);

//        @Language("SQL")
        String query = "SELECT " + allCols + ",ROWID as " + ROW_ID_COLUMN_NAME + " FROM " + from;

        PreparedStatement statement = connection.prepareStatement(query);
        statement.setFetchSize(DEFAULT_FETCH_SIZE);
        return statement;
    }

    Optional<BlockRange> importWholeTable(TableRef tableRef, List<OracleColumn> columns, ForEachRow forEach)
            throws ImportFailureException {
        ConnectionFactory.getInstance().retry(
                tableRef.toString(),
                t -> new ImportFailureException("Failed while performing initial import on " + tableRef, t),
                (conn) -> {
                    importWholeTable(tableRef, columns, forEach, conn);
                    return null;
                });
        return Optional.empty(); // empty means we were successful
    }

    private void importWholeTable(
            TableRef tableRef, List<OracleColumn> columns, ForEachRow forEach, Connection connection)
            throws SQLException {

        Instant startTime = ExampleClock.Instant.now();
        long rowCount = 0;
        try (PreparedStatement selectRows = selectImportRowsAll(connection, tableRef, columns);
             ResultSet rows = selectRows.executeQuery()) {

            while (rows.next()) {
                acceptRow(tableRef, rows, columns, forEach);

                rowCount++;
                if (rowCount % 100_000 == 0) {
                    LOG.info(
                            "Imported "
                                    + rowCount
                                    + " rows in "
                                    + Duration.between(startTime, ExampleClock.Instant.now()).toMillis()
                                    + " msecs");
                }
            }

            LOG.info(
                    "Imported all rows for table: "
                            + tableRef
                            + " ("
                            + rowCount
                            + " rows) in "
                            + Duration.between(startTime, ExampleClock.Instant.now()).toMillis()
                            + "msecs");
        }
    }

    /**
     * Fetch all rows from min ROWID to max ROWID
     */
    private PreparedStatement selectImportRowsFromRowIdRange(
            Connection connection, TableRef table, List<OracleColumn> columns, String minRowId, String maxRowId)
            throws SQLException {

        String from = doubleQuote(table);
        String allCols = getSelectCols(columns);

//        @Language("SQL")
        String query =
                "SELECT "
                        + allCols
                        + ", ROWID as "
                        + ROW_ID_COLUMN_NAME
                        + " FROM "
                        + from
                        + " WHERE ROWID BETWEEN ? and ?";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, minRowId);
        statement.setString(2, maxRowId);

        statement.setFetchSize(DEFAULT_FETCH_SIZE);
        return statement;
    }

    public void importBlockPageUsingRowIds(
            TableRef tableRef, List<OracleColumn> columns, BlockRange pageBlockRange, ForEachRow forEachRow)
            throws ImportFailureException {

        LOG.info("Starting import using Row Ids for table " + tableRef);
        List<Transactions.ObjectFileInfo> objectFileInfos = dbaExtendsHandler.getObjectFileInfoFromBlocks(tableRef, pageBlockRange);
        // If no rows on the table we're syncing exist in the given blockrange, skip this blockrange.
        if (objectFileInfos.isEmpty()) {
            LOG.info(
                    "There are no blocks matching the range "
                            + pageBlockRange.min
                            + " to "
                            + pageBlockRange.max
                            + " for table "
                            + tableRef);
            return;
        }

        for (Transactions.ObjectFileInfo singleObjectFileInfo : objectFileInfos) {

            String minRowId =
                    dbaExtendsHandler.getMinRowIdFromBlockInfo(tableRef, tableRowCountCache.getRowIdType(tableRef), singleObjectFileInfo, pageBlockRange);
            String maxRowId =
                    dbaExtendsHandler.getMaxRowIdFromBlockInfo(tableRef, tableRowCountCache.getRowIdType(tableRef), singleObjectFileInfo, pageBlockRange);

            LOG.info(
                    "Starting Row pages import for "
                            + tableRef
                            + " , object "
                            + singleObjectFileInfo.objectId
                            + " , file "
                            + singleObjectFileInfo.fileNumber);
            ConnectionFactory.getInstance().retry(
                    "Import rows using RowId ranges for " + tableRef,
                    t ->
                            new ImportFailureException(
                                    "Failed while performing initial import using Rowid range from "
                                            + minRowId
                                            + " to "
                                            + maxRowId
                                            + " on "
                                            + tableRef
                                            + "",
                                    t),
                    (conn) -> {
                        importBlockPageUsingRowIds(tableRef, columns, forEachRow, minRowId, maxRowId, conn);
                        return null;
                    });
        }
    }

    void importBlockPageUsingRowIds(
            TableRef tableRef,
            List<OracleColumn> columns,
            ForEachRow forEach,
            String minRowId,
            String maxRowId,
            Connection connection)
            throws SQLException {

        long rowCount = 0;
        try (PreparedStatement selectRows =
                     selectImportRowsFromRowIdRange(connection, tableRef, columns, minRowId, maxRowId);
             ResultSet rows = selectRows.executeQuery()) {

            while (rows.next()) {
                acceptRow(tableRef, rows, columns, forEach);
                rowCount++;
            }

            LOG.info(
                    "Imported RowId range "
                            + minRowId
                            + "api/src/main "
                            + maxRowId
                            + " for table: "
                            + tableRef
                            + " ("
                            + rowCount
                            + " rows)");
        }
    }

    void setFastImportLimit(long limit) {
        this.fastImportLimit = limit;
    }

    void setGuaranteedImportDuration(Duration importDuration) {
        this.guaranteedImportDuration = importDuration;
    }

    boolean stateIsEmpty() {
        return state.equals(new OracleState());
    }

    //    @Override
    public Service<?, OracleState> service() {
        return service;
    }

    void setState(boolean forceLogEvent) {
        if (forceLogEvent
                || Duration.between(lastImportProgressEvent, ExampleClock.Instant.now())
                .compareTo(LOG_IMPORT_PROGRESS_EVERY)
                > 0) {
            lastImportProgressEvent = ExampleClock.Instant.now();
            Set<String> completed =
                    (Set<String>) state.initialSyncComplete.stream().map(TableRef::toString).collect(Collectors.toSet());
            Set<String> inProgress =
                    state.remainingBlockRange.keySet().stream().map(TableRef::toString).collect(Collectors.toSet());
            LogEvent event = ImportProgressEvent.ofSimple(completed, inProgress);
            LOG.customerInfo(event);
        }

        output.checkpoint(state);
    }

    private void setState() {
        setState(false);
    }

    public void update2(Output<OracleState> output, StandardConfig standardConfig) {
        this.output = output;
        this.outputHelper = oracleConnectorContext.createOracleOutputHelper(output, standardConfig);

        Set<NoTableAccessType> needAccess = new HashSet<>();
        if (!Transactions.hasDbaExtentAccess.get()) needAccess.add(NoTableAccessType.DBA_EXTENTS);
        if (!Transactions.hasDbaTablespacesAccess.get()) needAccess.add(NoTableAccessType.DBA_TABLESPACES);
        if (!Transactions.hasDbaSegmentsAccess.get()) needAccess.add(NoTableAccessType.DBA_SEGMENTS);
        if (!needAccess.isEmpty()) {
            outputHelper.warn(new NoTableAccessWarning(params.schema, needAccess));
            LOG.customerWarning(
                    WarningEvent.warning(
                            "insufficient_permissions",
                            "In order to take advantage of a quicker initial import method, "
                                    + "you must give the connecting user SELECT permissions on the following views: "
                                    + needAccess.stream().map(Enum::toString).collect(Collectors.joining(", "))));
        }

        LOG.info("START");
        api.setWarningIssuer(outputHelper::warn);

        TagValue.SyncStatus syncStatus = SUCCESS;
        try {
            doUpdate();
            if (!state.remainingBlockRange.isEmpty()) {
                throw Rescheduled.retryAfter(Duration.ZERO, "Initial table sync is not complete yet.");
            }
        } catch (Exception e) {
            if (e instanceof InvalidScnException) {
                if (FeatureFlag.check("OracleReconnectOnInvalidScn")) {
                    syncStatus = COMPLETED_WITH_TASK;
                    throw CompleteWithTask.reconnect(e, "Fell behind archive log");
                }

                LOG.warning("Invalid SCN, rescheduling and resetting state...");

                state = new OracleState();
                setState(true);

                syncStatus = RESCHEDULED;
//                throw Rescheduled.retryAfter(
//                        Duration.ZERO, "We have fallen behind in an incremental update. Resyncing.");
            } else if (Exceptions.findCause(e, Rescheduled.class).isPresent()) {
                syncStatus = RESCHEDULED;
//            } else if (Exceptions.findCause(e, CompleteWithTask.class).isPresent()) {
//                syncStatus = COMPLETED_WITH_TASK;
            } else {
                syncStatus = FAILURE;
            }
//            throw e;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            publishMetrics(syncStatus);

            // TODO: no longer needed after standardized DB implementation
//            DbInfoLoggingUtils.logDbVersion(api::connectToSourceDb, service.serviceType);
            LOG.info("DONE");
        }
    }

    private void publishMetrics(TagValue.SyncStatus status) {
        LOG.info("publishing metrics to New Relic .....");
        metricsHelper.publishSyncEndMetrics(status);
    }

//    @Override
//    public void update(RecordProcessor<OracleState> recordProcessor, StandardConfig standardConfig) {
//        throw new UnsupportedOperationException("`update` with RecordProcessor no longer supported. Use `update2`.");
//    }

    void doUpdate() throws Throwable {
        Set<TableRef> selected = outputHelper.selectedTables();

        cleanUpState(selected);

        if (selected.isEmpty()) return;

        PartitionedTableCache partitionedTableCache = new PartitionedTableCache();
        // populate partition tables once.
        partitionedTableCache.initPartitionedTables(selected);

        Map<TableRef, List<OracleColumn>> selectedTables = api.columns(selected);
        List<TableRef> tablesToImport =
                selectedTables
                        .keySet()
                        .stream()
                        .filter(t -> !state.initialSyncComplete.contains(t))
                        .collect(Collectors.toList());

        // Initialize the cursor here. It would be better to initialize the cursor right before starting
        // initial import of each table. Will leave it as a future improvement.
        Long currentScn = null;
        if (!FlagName.OracleSkipIncrementalSyncBeforeImportStarts.check() && useFlashback) {
            for (TableRef table : tablesToImport) {
                if (state.getTableSyncedScn(table) == null) {
                    if (currentScn == null) currentScn = databaseDao.getCurrentScn();
                    state.setTableSyncedScn(table, currentScn);
                }
            }
        }
        if (!useFlashback
                && FlagName.OracleSkipIncrementalWhenNothingToImport.check()
                && !state.earliestUncommitedScn.isPresent()) {
            long start = state.earliestUncommitedScn.orElseGet(api::mostRecentScn);
            state.earliestUncommitedScn = Optional.of(start);
            output.checkpoint(state);
        }

        // pre-generate the table definitions before we start the sync
        outputHelper.saveTableDefs(selectedTables, new HashSet<>(tablesToImport));

        int totalTables = selectedTables.size();
        int importedTables = totalTables - tablesToImport.size();

        // Order is important during the first run!
        // You must sync the logs BEFORE you sync existing data,
        // otherwise you could miss updates between syncing the existing data and syncing the logs
        this.incrementalUpdater = oracleConnectorContext.newIncrementalUpdater(selectedTables);
        incrementalUpdater.start();
        try {
            // Refresh the tally before the incremental update (tables may have been added/removed)
            if (!tablesToImport.isEmpty())
                DbImportProgress.logMetricAndUfl(tablesToImport.get(0).toString(), importedTables, totalTables);

            if (!useFlashback && !FlagName.OracleSkipIncrementalWhenNothingToImport.check()) {
                incrementalUpdate(selectedTables);
            }

            Instant importPageGuaranteedUntil = Instant.now().plus(guaranteedImportDuration);

            for (TableRef table : tablesToImport) {
                DbImportProgress.logMetricAndUfl(table.toString(), importedTables++, totalTables);

                List<OracleColumn> columns = selectedTables.get(table);

                try {
                    while (!state.tableImportComplete(table)) {
                        if (state.isBeforeStartingTableImport(table)) {
                            Instant syncStart = ExampleClock.Instant.now();
                            state.syncBeginTime.put(table, syncStart);
                            if (FlagName.OracleSkipIncrementalSyncBeforeImportStarts.check()) {
                                long scn = databaseDao.getCurrentScn();
                                state.setTableSyncedScn(table, scn);
                            }
                        }

                        // sync a single page of the table
                        syncTablePage(table, columns);

                        if (FeatureFlag.check("OracleDoNotInterleaveIncrementalSyncForFixedDuration")) {
                            // Interleave incremental sync only after FIXED_IMPORT_DURATION (instead of every page)
                            if (Instant.now().isAfter(importPageGuaranteedUntil)) {
                                LOG.info("Guaranteed Import duration has passed, switching to incremental update....");
                                // sync logs again
                                incrementalUpdate(selectedTables);
                                importPageGuaranteedUntil = Instant.now().plus(guaranteedImportDuration);
                            }
                        } else {
                            // sync logs again
                            incrementalUpdate(selectedTables);
                        }
                    }
                } catch (MissingObjectException e) {
                    LOG.log(
                            Level.WARNING,
                            "Access to " + table + " lost. Removing it from initialSyncComplete in state.",
                            e);
                    state.resetTable(table);
                    selectedTables.remove(table);
                    setState();
                }
            }

            incrementalUpdater.stop();
            incrementalUpdate(selectedTables);

        } finally {
            incrementalUpdater.close();
        }

        warnAboutBadVals(selectedTables);

        int resyncTableCount = resyncHelper.countTablesToResync();
        if (resyncTableCount > 0) {
            throw Rescheduled.retryAfter(Duration.ZERO, resyncTableCount + " tables need to be re-synced.");
        }
    }

    void incrementalUpdate(Map<TableRef, List<OracleColumn>> selectedTables) {
        long extractedDataVolUntilNow = outputHelper.getExtractVol();

        if (FlagName.OracleSkipIncrementalWhenNothingToImport.check()) {
            if (!requireRunningIncrementalSync) {
                return; // We don't want to execute incremental update twice in a row
            }
            requireRunningIncrementalSync = false;
        }

        if (useFlashback) {
            try {
                incrementalUpdater.doIncrementalWork();
            } catch (OracleLogStreamException e) {
                throw new RuntimeException(e);
            }
        } else {
            syncLogs(selectedTables);
        }
//        OracleMetricsHelper.logExtractDataVolume(
//                "OracleIncrementalChanges",
//                (outputHelper.getExtractVol() - extractedDataVolUntilNow),
//                output.getLastFlushedTime());
    }

    private void warnAboutBadVals(Map<TableRef, List<OracleColumn>> selectedTables) {

        List<String> badColumnMessages = new ArrayList<>();
        selectedTables.forEach(
                (tableRef, oracleColumns) ->
                        oracleColumns
                                .stream()
                                .filter(oc -> !oc.badDateVals.isEmpty())
                                .forEach(
                                        oc ->
                                                badColumnMessages.add(
                                                        oc.badDateVals.size()
                                                                + " value(s) from "
                                                                + tableRef.toString()
                                                                + "."
                                                                + oc.name)));

        if (!badColumnMessages.isEmpty()) {

//            String message =
//                    "We had trouble parsing values from the following columns:\n"
//                            + Joiner.on("\n").join(badColumnMessages)
//                            + "\n";
//            message += "These values were replaced with NULL.";
//
//            LOG.warning(message);
        }
    }

    private void cleanUpState(Set<TableRef> selected) {
        // If the user unselected any tables, remove them from state.initialSyncComplete, indicating they
        // would need to be re-synced if the user selects them again
        state.initialSyncComplete.removeIf(t -> !selected.contains(t));

        // if the user stopped the sync and unselected a table that was in the middle being synced, it would show
        // that there are still blocks remaining to import, so we need to clear them from the state too
//        state.remainingBlockRange.entrySet().removeIf(t -> !selected.contains(t.getKey()));
//        state.syncBeginTime.entrySet().removeIf(t -> !selected.contains(t.getKey()));
    }

    void syncLogs(Map<TableRef, List<OracleColumn>> selected) {
        if (!state.earliestUncommitedScn.isPresent() && !state.initialSyncComplete.isEmpty()) {
            throw new IllegalStateException(
                    "You have started syncing logs AFTER syncing a table! "
                            + "The synced tables may contain updates that we will never see if we starting syncing from here. "
                            + "You need to sync logs BEFORE syncing data from any tables. "
                            + "If this is a resync, you should clear state.initialSyncComplete before running syncLogs. ");
        }

        long start =
                FlagName.OracleSkipIncrementalWhenNothingToImport.check()
                        ? state.earliestUncommitedScn.get()
                        : state.earliestUncommitedScn.orElseGet(api::mostRecentScn);

        Transactions.ChangedRowsResult result = doSyncLogs(selected, start);

        state.earliestUncommitedScn = Optional.of(result.earliestUncommittedScn);
        state.dynamicPageSize = Optional.of(result.lastPageSize);

        resyncHelper.warnAboutResync();
        setState();
    }

    public Transactions.ChangedRowsResult doSyncLogs(Map<TableRef, List<OracleColumn>> selected, long since) {
        return logMinerIncrementalUpdater.changedRows(
                since,
                state.dynamicPageSize,
                selected,
                (badTable, reason) -> resync(badTable, reason, false),
                tableRef -> outputHelper.preImportDelete(tableRef, ExampleClock.Instant.now()),
                (target, rowId, row, change, maybeRowPrimaryKeys) -> {
                    incrementalUpdater.forEachChange(
                            target, rowId, row, change, Instant.EPOCH, maybeRowPrimaryKeys, selected);
                },
                scn -> {
                    state.earliestUncommitedScn = Optional.of(scn);
                    setState();
                });
    }

    protected void resync(TableRef badTable, String reason, Boolean logEventNow) {
        incrementalUpdater.resync(badTable, reason, logEventNow);
    }

    private ForEachRow getForEachRow(TableRef table, List<OracleColumn> columns, Instant importBeginTime) {
        List<Column> incomingColumns =
                columns.stream().map(c -> c.asexampleColumn(table.schema)).collect(Collectors.toList());
        List<Column> exampleColumns = new ArrayList<>(incomingColumns);

        boolean isPkeylessTable = incomingColumns.stream().noneMatch(c -> c.primaryKey);
        if (isPkeylessTable) {
//            exampleColumns.add(Column.String(Names.example_ID_COLUMN).primaryKey(true).build());
        }

        Set<TableRef> partitionedTables = api.partitionedTablesWithRowMovementEnabled();

        return (rowId, row) -> {


            metricsHelper.incrementRowsImported();
            Map<String, Object> exampleRow = row;
            exampleRow.put(Names.example_DELETED_COLUMN, false);
            exampleRow.put(Names.example_SYNCED_COLUMN, ExampleClock.Instant.now());

            if (isPkeylessTable) {
//                if (partitionedTables.contains(table))
//                    exampleRow.put(Names.example_ID_COLUMN, hashIdGenerator.hashId(row, incomingColumns));
//                else exampleRow.put(Names.example_ID_COLUMN, rowId);
            }
            outputHelper.submitRecord(exampleRow, table, exampleColumns, importBeginTime, ChangeType.INSERT);
        };
    }

    Optional<BlockRange> determineTableBlockRange(TableRef tableRef, Optional<BlockRange> remainingBlockRange) {
        BlockRange tableBlockRange;
        if (remainingBlockRange.isPresent()) {
            tableBlockRange = remainingBlockRange.get();

            // migration for tables in the middle of the import
            if (tableBlockRange.type == null) {
                // compare the block range from state with the block range from the new detect method.
                Optional<BlockRange> maybeTableBlockRange = dbaExtendsHandler.getTableBlockRange(tableRef);
                if (!maybeTableBlockRange.isPresent()) {
                    LOG.info("Table is empty: " + tableRef);
                    return Optional.empty();
                }

                BlockRange newBlockRange = maybeTableBlockRange.get();

                if (newBlockRange.min <= tableBlockRange.min && tableBlockRange.max == newBlockRange.max) {
                    /*
                     * The block range in state is within the newly computed range AND has the same end, we can consider
                     * the existing block range as accurate and set the tablespace type into it
                     */
                    tableBlockRange.type = newBlockRange.type;
                } else {
                    /*
                     * Unfortunately there is no way to verify with any certainty that the old range was using the
                     * correct tablespace type. We have to start the table over. We can do that just by starting with
                     * the new block range.
                     */
                    tableBlockRange = newBlockRange;
                }
            }
        } else {
            Optional<BlockRange> maybeTableBlockRange = dbaExtendsHandler.getTableBlockRange(tableRef);
            if (!maybeTableBlockRange.isPresent()) {
                return Optional.empty();
            }
            tableBlockRange = maybeTableBlockRange.get();
        }
        return Optional.of(tableBlockRange);
    }

    void acceptRow(TableRef table, ResultSet rows, List<OracleColumn> columns, ForEachRow forEach) throws SQLException {
        Map<String, Object> row = new HashMap<>();

        // queryFirst column needs to be read first before all other columns.
        // Otherwise it throws "Stream has already been closed" error
        // LONG is the only column type that exhibits this behavior
        columns.stream()
                .filter(c -> c.oracleType.isLong())
                .forEach(
                        c -> {
                            Object value = outputHelper.coerce(table, rows, c);
                            row.put(c.name, value);
                        });

        // now read the other columns
        if (FlagName.OracleReplaceInvaldNumberWithNULL.check()) {
            StringBuilder errorMsg = null;
            for (OracleColumn c : columns) {
                if (c.oracleType.isLong()) continue;
                try {
                    Object value = outputHelper.coerce(table, rows, c);
                    row.put(c.name, value);
                } catch (Exception e) {
                    Throwable cause = e.getCause();
                    if (cause.getMessage() != null && cause.getMessage().contains("Numeric Overflow")) {
                        if (invalidNumberWarningCount < MAX_INVALID_NUMBER_WARNING_BEFORE_GIVING_UP) {
                            Object value = rows.getString(c.name);
                            if (errorMsg == null) {
                                errorMsg = new StringBuilder();
                                errorMsg.append("Error in Table: " + table + "\n");
                            }
                            errorMsg.append("Cannot read row " + c.name + " VALUE = " + value + ".\n");
                            errorMsg.append("The destination value will be set to NULL.\n");
                            errorMsg.append("Please confirm and correct the following value in the source table.\n");
                            errorMsg.append(
                                    "SELECT "
                                            + c.name
                                            + " FROM "
                                            + doubleQuote(table)
                                            + " WHERE ROWID = "
                                            + singleQuote(rows.getString(ROW_ID_COLUMN_NAME)));
                            row.put(c.name, null);
                            invalidNumberWarningCount++;
                            continue;
                        } else {
                            LOG.customerInfo(
                                    InfoEvent.of(
                                            "warning",
                                            "Giving up sync ... Too many invalid numbers. Please correct values before retry."));
                        }
                    }
                    throw e;
                }
            }
            if (errorMsg != null) {
                LOG.customerInfo(InfoEvent.of("warning", errorMsg.toString()));
            }
        } else {
            columns.stream()
                    .filter(c -> !c.oracleType.isLong())
                    .forEach(
                            c -> {
                                Object value = outputHelper.coerce(table, rows, c);
                                row.put(c.name, value);
                            });
        }

        String rowId = rows.getString(ROW_ID_COLUMN_NAME);

        forEach.accept(rowId, row);
    }

    /**
     * Fetch all rows belonging to block numbers `keyStart` to `keyEnd`, inclusive
     */
    @SuppressWarnings("java:S2095")
    private PreparedStatement selectImportRows(
            Connection connection, TableRef table, List<OracleColumn> columns, BlockRange pageBlockRange)
            throws SQLException {

        String from = doubleQuote(table);
        String allCols = getSelectCols(columns);

//        @Language("SQL")
        String query =
                "SELECT "
                        + allCols
                        + ",ROWID as "
                        + ROW_ID_COLUMN_NAME
                        + " FROM "
                        + from
                        + " WHERE DBMS_ROWID.ROWID_BLOCK_NUMBER(ROWID, ?) BETWEEN ? and ?";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, pageBlockRange.type.toString());
        statement.setBigDecimal(2, new BigDecimal(pageBlockRange.min));
        statement.setBigDecimal(3, new BigDecimal(pageBlockRange.max));

        statement.setFetchSize(DEFAULT_FETCH_SIZE);
        return statement;
    }

    private void importBlockRange(
            TableRef tableRef,
            List<OracleColumn> columns,
            ForEachRow forEach,
            BlockRange pageBlockRange,
            Connection connection)
            throws SQLException {

        long blockRowCount = 0;
        try (PreparedStatement selectRows = selectImportRows(connection, tableRef, columns, pageBlockRange);
             ResultSet rows = selectRows.executeQuery()) {

            while (rows.next()) {
                acceptRow(tableRef, rows, columns, forEach);
                blockRowCount++;
            }

            LOG.info(
                    "Imported block range "
                            + pageBlockRange.min
                            + "api/src/main "
                            + pageBlockRange.max
                            + " for table: "
                            + tableRef
                            + " ("
                            + blockRowCount
                            + " rows)");
        }
    }

    public void importBlockPage(
            TableRef tableRef, List<OracleColumn> columns, BlockRange pageBlockRange, ForEachRow forEachRow)
            throws ImportFailureException {

        ConnectionFactory.getInstance().retry(
                tableRef.toString(),
                t -> new ImportFailureException("Failed while performing initial import on " + tableRef + "", t),
                (conn) -> {
                    importBlockRange(tableRef, columns, forEachRow, pageBlockRange, conn);
                    return null;
                });
    }

    public void syncTablePage(TableRef table, List<OracleColumn> columns) {
        LOG.info(ReadEvent.start(table.toString()));
        if (FlagName.OracleSkipIncrementalWhenNothingToImport.check()) {
            requireRunningIncrementalSync = true;
        }

        // Since Oracle's SCN, which embodies a representation of the time, is only accurate to the true time
        // within 3 seconds, set the import time (for History Mode) to 3 seconds earlier than the SCN time.
        // That way, no incremental updates for this table can accidentally be timestamped earlier than the imported
        // data.
        Instant importBeginTime = ExampleClock.Instant.now().minus(Duration.ofSeconds(ORACLE_SCN_PRECISION));

        long rowCount;
        try {
            rowCount = api.getTableRowCountCache().getRowCount(table);
        } catch (ImportFailureException e) {
            LOG.log(Level.INFO, "Halting import after three failed attempts", e);
            LOG.info(ReadEvent.end(table.toString()));
            return;
        }

        if (rowCount == 0) {
            LOG.info("Table is empty: " + table);
            // Mark the table import complete.
            state.initialSyncComplete.add(table);
            state.remainingBlockRange.remove(table);
            setState();
            LOG.info(ReadEvent.end(table.toString()));
            return;
        }

        Instant importPageStart = ExampleClock.Instant.now();
        Optional<BlockRange> maybeTableBlockRange =
                determineTableBlockRange(table, Optional.ofNullable((BlockRange) state.remainingBlockRange.get(table)));

        if (!maybeTableBlockRange.isPresent()) {
            LOG.info("Table is empty: " + table);
            // Mark the table import complete.
            state.initialSyncComplete.add(table);
//            state.remainingBlockRange.remove(table);
            setState();
            LOG.info(ReadEvent.end(table.toString()));
            return;
        }

        long extractedDataVolUntilNow = outputHelper.getExtractVol();
        BlockRange tableBlockRange = maybeTableBlockRange.get();

        long tableBlockSize = Util.calculateBlockSize(tableBlockRange);

        LOG.info(
                "Remaining block range: "
                        + tableBlockRange.min
                        + "api/src/main "
                        + tableBlockRange.max
                        + " (block size: "
                        + tableBlockSize
                        + ")");

        BlockRange nextPageBlockRange =
                tableBlockRange.withMax(Math.min(tableBlockRange.max, tableBlockRange.min + tableBlockSize));
        Optional<BlockRange> remainingBlockRange = Optional.empty();

        ForEachRow forEachRow = getForEachRow(table, columns, importBeginTime);

        try {

            boolean useImportWholeTable =
                    FeatureFlag.check("OracleBruteImportTableInOneGo") || (rowCount <= fastImportLimit);

            if (useImportWholeTable) {
                remainingBlockRange = importWholeTable(table, columns, forEachRow);
            } else {

                if (FeatureFlag.check("OracleUseRowIdRangeForImport")) {
                    importBlockPageUsingRowIds(table, columns, nextPageBlockRange, forEachRow);
                } else {
                    importBlockPage(table, columns, nextPageBlockRange, forEachRow);
                }
                // more pages to process
                if (nextPageBlockRange.max < tableBlockRange.max) {
                    remainingBlockRange = Optional.of(tableBlockRange.withMin(nextPageBlockRange.max));
                }
            }

//        } catch (ImportFailureException e) {
        } catch (Exception e) {
            // we'll try again later
            LOG.info(
                    "Halting import after three failed attempts. Saving progress at block range: "
                            + nextPageBlockRange.min
                            + " ... "
                            + tableBlockRange.max
                            + " for table "
                            + table);
            remainingBlockRange = Optional.of(tableBlockRange.withMin(nextPageBlockRange.min));
        } finally {
            metricsHelper.recordImportDuration(Duration.between(importPageStart, ExampleClock.Instant.now()));
        }

        if (remainingBlockRange.isPresent()) {
            state.remainingBlockRange.put(table, remainingBlockRange.get());
        } else {
            // Table import is complete
            state.initialSyncComplete.add(table);
//            state.remainingBlockRange.remove(table);
//            if (!state.syncBeginTime.containsKey(table)) {
//                throw new RuntimeException("Table sync completed, but syncBeginTime is missing.");
//            }
//            outputHelper.preImportDelete(table, (Instant) state.syncBeginTime.get(table));
        }

        setState();
        OracleMetricsHelper.logExtractDataVolume(
                "OracleImportChanges",
                (outputHelper.getExtractVol() - extractedDataVolUntilNow),
                output.getLastFlushedTime());
        LOG.info(ReadEvent.end(table.toString()));
    }

    //    @Override
    public void close() throws CloseConnectionException {
        LOG.info("Closing OracleUpdater, OracleApi");
        api.close();
    }

}