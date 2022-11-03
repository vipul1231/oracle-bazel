package com.example.oracle;

import com.example.core.Column;
import com.example.core.CompleteWithTask;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.db.DbQuery;
import com.example.db.spi.AbstractHashIdGenerator;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.micrometer.MetricDefinition;
import com.example.oracle.exceptions.InvalidScnException;
import com.example.oracle.exceptions.TypePromotionNeededException;
import com.example.oracle.spi.OracleAbstractIncrementalUpdater;
import com.example.oracle.tasks.InsufficientFlashbackStorage;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.example.core.annotations.DataType.Instant;
import static com.example.core.annotations.DataType.String;
import static com.example.oracle.ChangeType.*;
import static com.example.oracle.Constants.DEFAULT_FETCH_SIZE;
import static com.example.oracle.OracleErrorCode.*;
import static com.example.oracle.OracleService.LOG;
import static com.example.oracle.SqlUtil.getSelectCols;
import static com.example.oracle.Util.doubleQuote;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/7/2021<br/>
 * Time: 10:19 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleIncrementalUpdater implements OracleAbstractIncrementalUpdater {
    final OracleState state;
    final OracleApi api;

    protected OracleOutputHelper outputHelper;

//    private AbstractHashIdGenerator hashIdGenerator;

    final OracleResyncHelper resyncHelper;
    private final OracleMetricsHelper metricsHelper;

    protected final Map<TableRef, List<OracleColumn>> includedTables;
    private final Set<TableRef> partitionedTablesWithRowMovementEnabled;
    private final Map<TableRef, List<String>> columnNamesWithDateType;
    private final Set<TableRef> pKeylessTables;

    protected final Boolean oracleSkipVersionsStarttime;

    static final int MAX_RETRIES = 3;
    private static final int QUERY_LIST_MAX_SIZE = 1000;

    protected final Set<TableRef> reSyncTables = ConcurrentHashMap.newKeySet();

    //    private final SimpleCache<Long, Instant> scnToTimestampCache = new SimpleCache<>(this::scnToTimestamp);
    private final SimpleCache<Long, Instant> scnToTimestampCache = null;

    private static final DateTimeFormatter DDL_TIMESTAMP_FORMAT;

    /**
     * this flag indicates that the first flashback incremental sync performed. After the first run, reset to false
     */
    boolean isFirstRun = true;

    static {
        DDL_TIMESTAMP_FORMAT =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        // custom Oracle separator
                        .appendLiteral(':')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter()
                        .withZone(ZoneId.of("UTC"));
    }

    public class TableChangeRow {
        ResultSet resultSet;

        public TableChangeRow(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        public Long getVersionsStartScn() throws SQLException {
            return resultSet.getLong("VERSIONS_STARTSCN");
        }

        public String getVersionsOperation() throws SQLException {
            return resultSet.getString("VERSIONS_OPERATION");
        }

        public String getRowId() throws SQLException {
            return resultSet.getString("ROWID");
        }

        public Instant getVersionsStartTime(SyncMode syncMode) throws SQLException {
            if (!oracleSkipVersionsStarttime || syncMode == SyncMode.History) {
                return resultSet.getTimestamp("VERSIONS_STARTTIME").toInstant();
            }

            return ExampleClock.Instant.now();
        }
    }

    OracleIncrementalUpdater(
            OracleState state,
            OracleApi api,
//            AbstractHashIdGenerator hashIdGenerator,
            AbstractHashIdGenerator hashIdGenerator, OracleOutputHelper outputHelper,
            OracleResyncHelper resyncHelper,
            OracleMetricsHelper metricsHelper,
            Map<TableRef, List<OracleColumn>> selected) {
        // to be removed in T-78067
        api.setOutputHelper(outputHelper);
        this.state = state;
        this.api = api;
        this.outputHelper = outputHelper;
//        this.hashIdGenerator = hashIdGenerator;
        this.resyncHelper = resyncHelper;
        this.metricsHelper = metricsHelper;

        this.includedTables = ImmutableMap.copyOf(selected);
        this.partitionedTablesWithRowMovementEnabled = api.partitionedTablesWithRowMovementEnabled();
//        this.columnNamesWithDateType =
//                Maps.transformEntries(selected, OracleIncrementalUpdater::retrieveColumnNamesWithDateType);
        this.columnNamesWithDateType = null;
        this.pKeylessTables = primaryKeylessTables(selected);
        oracleSkipVersionsStarttime = FeatureFlag.check("OracleSkipVersionsStarttime");
        LOG.info("OracleSkipVersionsStarttime flag enabled=" + oracleSkipVersionsStarttime);
    }

    protected boolean checkSkipFlashbackUpdate(TableRef tableRef) {
        return resyncHelper.tableNeedsResync(tableRef);
    }

    protected void doCheckpoint() {
        outputHelper.checkpoint(state);
    }

    /**
     * Iterate through included tables, performing an incremental update on each
     */
    protected void flashbackUpdate() {
        long currentScn = api.getDatabaseDao().getCurrentScn();
        List<TableRef> tableList = new ArrayList<>(includedTables.keySet());
        Map<TableRef, Instant> lastDdlTimes = lastDdlTimes(tableList, QUERY_LIST_MAX_SIZE);
        for (TableRef tableRef : includedTables.keySet()) {
            if (checkSkipFlashbackUpdate(tableRef)) {
                LOG.info("Skipping incremental update of " + tableRef.name);
                continue;
            }
            Long tableStartScn = getTableStartScn(tableRef, currentScn);
            if (tableStartScn == null) {
                continue;
            }
            if (currentScn > tableStartScn) {
                LOG.info(String.format("Updating table %s : %s .. %s", tableRef, tableStartScn, currentScn));
                Instant lastDdlTime = lastDdlTimes.get(tableRef);
                try {
                    long syncedUpToScn = updateTable(tableRef, currentScn, tableStartScn, lastDdlTime);
                    saveTableStartScn(tableRef, syncedUpToScn);
                } catch (InvalidScnException e) {
                    resync(tableRef, e.getMessage(), true);
                }
            } else {
                LOG.info(
                        String.format(
                                "Table %s needs to be imported or has no new changes; skipping incremental update.",
                                tableRef));
            }

            doCheckpoint();
        }
        // end of the first run
        isFirstRun = false;
    }

    /**
     * Post this duration as an individual ORACLE_VERSION_QUERY_DURATION and add to overall LOG_EXTRACTION_DURATION *
     */
    void saveVersionQueryExtractionMetrics(Duration duration) {
        metricsHelper.addToLogExtractionDuration(duration);
        metricsHelper.publishVersionQueryExtraction(duration);
    }

    /**
     * Update in state
     */
    protected void saveTableStartScn(TableRef tableRef, long syncedUpToScn) {
        state.setTableSyncedScn(tableRef, syncedUpToScn);
    }

    // Should remove currentScn argument when cleaning up OracleSkipIncrementalSyncBeforeImportStarts FF
    // along with the following comment.

    /**
     * For the first time, set SCN in state and return the same SCN, Otherwise, provide the existing SCN
     */
    protected Long getTableStartScn(TableRef tableRef, long currentScn) {
        if (FlagName.OracleSkipIncrementalSyncBeforeImportStarts.check()) {
            return state.getTableSyncedScn(tableRef);
        } else {
            return state.initTableSyncedScn(tableRef, currentScn);
        }
    }

    @SuppressWarnings("java:S2095")
    PreparedStatement getIncrementalChangesForTable(
            Connection connection,
            TableRef table,
            List<OracleColumn> columns,
            Long startScn,
            Long endScn,
            SyncMode syncMode)
            throws SQLException {
        String tableString = doubleQuote(table);
        String allCols = getSelectCols(columns);

        /*
         NOTE : We used to set start and end SCN using prepared statement but it used to throw ORA-01466.
         Forming the query with appending these values removed that case.
         VERSIONS_STARTTIME is only used for history mode that let us save query execution time for converting scn
         to timestamp in non-history mode.
        */
//        @Language("SQL")
        String query =
                "SELECT "
                        + (FlagName.OracleUseOptimizedVersionQuery.check() ? api.getParallelHint(table) : "")
                        + "VERSIONS_STARTSCN, "
                        + ((!FeatureFlag.check("OracleSkipVersionsStarttime") || syncMode == SyncMode.History)
                        ? " VERSIONS_STARTTIME, "
                        : "")
                        + "VERSIONS_OPERATION, ROWID, "
                        + allCols
                        + " FROM "
                        + tableString
                        + " VERSIONS BETWEEN SCN "
                        + startScn
                        + " AND "
                        + endScn
                        + " WHERE VERSIONS_OPERATION IS NOT NULL"
                        + " AND VERSIONS_STARTSCN > "
                        + startScn
                        + " AND VERSIONS_STARTSCN <= "
                        + endScn;

        if (!FeatureFlag.check("OracleFlashbackNoOrderBy")) {
            query +=
                    " ORDER BY VERSIONS_STARTSCN"
                            + (FlagName.OracleSortVersionsOperation.check()
                            ? ", decode(VERSIONS_OPERATION, 'D', 1, 'I', 2, 'U', 3)"
                            : "")
                            + " ASC";
        }

        // this query is buggy on the Oracle side when string formatting is used - please keep as concatenation
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setFetchSize(DEFAULT_FETCH_SIZE);
        return stmt;
    }

    /**
     * Perform table incremental update, retrying on failure
     *
     * @return Long representing the SCN of the most recently-synced change
     */
    long updateTable(TableRef table, Long currentScn, Long tableSyncedScn, Comparable<Instant> lastDdlTime) {
        Instant lastSyncedTimestamp = scnToTimestampCache.get(tableSyncedScn);
        SyncMode syncMode = outputHelper.syncModes().get(table);
        if (lastDdlTime.compareTo(lastSyncedTimestamp) > 0) { // resync if table definition has changed
            LOG.info(
                    "Last DDL time for table "
                            + table.toString()
                            + " of "
                            + lastDdlTime
                            + " is after last synced timestamp of "
                            + lastSyncedTimestamp
                            + "; resyncing");
            throw new InvalidScnException(String.format("Table definition has changed for %s", table));
        }

        String operand = String.format("Incremental update for table %s", table);

        if (FeatureFlag.check("OracleFlashbackSkipNoChangeTables") && !state.wasTableUpdated(table)) {
            // If there was no data update during the previous run, let us assume the trend continues. If not,
            // there is more chance that this table is active. Let us skip this check as it could just end up with an
            // unnecessary overhead.
            try {

                Optional<Long> maybeLastDmlScn = lastTableDmlScn(table);
                long lastDmlScn = maybeLastDmlScn.orElse(tableSyncedScn);
                if (lastDmlScn < tableSyncedScn) {
                    LOG.info(
                            "SCN of last DML ("
                                    + lastDmlScn
                                    + ") is before the SCN saved in state for table "
                                    + table.toString()
                                    + " ("
                                    + tableSyncedScn
                                    + ")");
                    return currentScn;
                }
            } catch (Exception e) {
                // We should not fail when this optional feature fails. Ignore any failure. if it should failure,
                // it will fail below while processing flashback.
                LOG.log(Level.WARNING, "lastTableDmlScn call failed and ignored.", e);
            }
        }

        Function<Exception, RuntimeException> finalExceptionWrapper =
                t -> {
                    // ORA-01555: snapshot too old
                    if (ORA_01555.is(t.getMessage())) {
                        return CompleteWithTask.create(new InsufficientFlashbackStorage(), t);
                    }

                    return new RuntimeException(
                            "Failed to perform " + operand + " after " + MAX_RETRIES + " attempts", t);
                };

        LOG.info("SyncMode for table " + table + " is " + syncMode);
        return ConnectionFactory.getInstance().retryWithFailureCounter(
                operand,
                finalExceptionWrapper,
                conn -> {
                    Instant versionQueryStart = java.time.Instant.now();
                    try (DbQuery statement =
                                 new DbQuery(
                                         getIncrementalChangesForTable(
                                                 conn,
                                                 table,
                                                 includedTables.get(table),
                                                 tableSyncedScn,
                                                 currentScn,
                                                 syncMode));
                         ResultSet rowsResult = statement.executeQuery()) {
                        // wasUpdated flag is only checked and set during the first run of the incremental sync.
                        return ingestTableChangeRows(rowsResult, table, currentScn, syncMode, versionQueryStart);
                    } catch (SQLException e) {
                        if (ORA_00904.is(e) // ORA-00904: invalid identifier -> column name not found in source
                                || ORA_01466.is(e) // ORA-01466: table definition has changed
                        ) {
                            LOG.info("Versions query returned " + e.getMessage() + "; resyncing");
                            throw new InvalidScnException(String.format("Table definition has changed for %s", table));
                        } else if (ORA_08181.is(e)) { // ORA-08181: specified number is not a valid system change number
                            LOG.info("Versions query returned " + e.getMessage() + "; resyncing");
                            throw new InvalidScnException(
                                    String.format("Invalid SCN, we have fallen behind in update for table %s", table));
                        } else if (ORA_30052.is(e)) {
                            // ORA-30052: invalid lower limit snapshot expression
                            String msg =
                                    "We have fallen behind in an incremental update and will restart. Oracle error: "
                                            + e;
//                            LOG.customerWarning(
//                                    ForcedResyncEvent.ofConnector(ForcedResyncEvent.ResyncReason.STRATEGY, msg));
                            state.resetAll();
                            outputHelper.checkpoint(state);
//                            throw Rescheduled.retryAfter(Duration.ZERO, msg);
                        }
                        throw e;
                    }
                },
                MetricDefinition.ORACLE_VERSION_QUERY_FAILURE_COUNT);
    }

    /**
     * Get the upper bound on the SCN of the last DML on this table. It's an upper bound because ORA_ROWSCN is changed
     * for an entire block any time a row changes - so we may infer that the table has had recent DML performed on it
     * when it hasn't.
     *
     * @param table
     * @return
     */
    Optional<Long> lastTableDmlScn(TableRef table) throws Exception {
        try (Connection con = api.connectToSourceDb();
//             DbQuery statement = new DbQuery(api.getLastDmlScnForTable(con, table));
             DbQuery statement = null;
             ResultSet rowsResult = statement.executeQuery()) {
            if (!rowsResult.next()) throw new SQLException("MAX(ORA_ROWSCN) query did not execute successfully");

            // we do this goofy getObject here instead of getLong because getLong returns 0 if there is no result.
            // we want to differentiate between no result (bad query, maybe no permissions), null (no rows in table),
            // and a legitimate result (some positive whole-number value)
            Object object = rowsResult.getObject("MAX(ORA_ROWSCN)");
            if (object == null) return Optional.empty();
            else
                // turns out this is actually a BigDecimal in the DB but it's easier to think of as a Long
                return Optional.of(rowsResult.getLong("MAX(ORA_ROWSCN)"));
        }
    }

    /**
     * Retrieve the time of the last table-changing DDL statement on the tables
     *
     * @param tables
     * @param maxSize number of rows per page
     * @return
     */
    Map<TableRef, Instant> lastDdlTimes(List<TableRef> tables, int maxSize) {
        List<List<TableRef>> tablePages = Lists.partition(tables, maxSize);
        return tablePages
                .stream()
                .map(this::lastDdlTimesPage)
                .collect(Collectors.toList())
                .stream()
                .flatMap(e -> e.entrySet().stream())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (t1, t2) -> { // there should not be duplicates here
                                    LOG.warning(
                                            "Duplicate key " + t1 + " found while splitting DDL time query into pages");
                                    return t2;
                                }));
    }

    /**
     * Retrieve the ALL_OBJECTS.TIMESTAMP column for each of the provided tables.
     * https://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2005.htm#i1583352
     *
     * @return a map of table name to timestamp indicating time of last table-changing DDL
     */
    Map<TableRef, Instant> lastDdlTimesPage(Collection<TableRef> tables) {
        return ConnectionFactory.getInstance().retry(
                "lastDdlTimes",
                conn -> {
                    try (DbQuery stmt = new DbQuery(api.getLastTableChangeTimestamps(conn, tables));
                         ResultSet result = stmt.executeQuery()) {
                        Map<TableRef, Instant> tableToTimestamp = new HashMap<>();
                        while (result.next()) {
                            String ownerName = result.getString("OWNER");
                            String tableName = result.getString("OBJECT_NAME");
                            TableRef table = new TableRef(ownerName, tableName);
                            String timestampString = result.getString("TIMESTAMP");
                            Instant timestamp = DDL_TIMESTAMP_FORMAT.parse(timestampString, Instant::from);
                            tableToTimestamp.put(table, timestamp);
                        }
                        return tableToTimestamp;
                    }
                });
    }

    protected void doForEachChange(
            TableChangeRow tableChangeRow, TableRef table, Map<String, Object> row, SyncMode syncMode)
            throws SQLException {
        forEachChange(
                table,
                tableChangeRow.getRowId(),
                row,
                ChangeType.flashbackChangeType(tableChangeRow.getVersionsOperation()),
                tableChangeRow.getVersionsStartTime(syncMode),
                Optional.empty(),
                includedTables);
    }

    /**
     * Process changes and pass to Output
     *
     * @return Long representing the SCN of the most recently-synced change
     * @throws SQLException
     */
    Long ingestTableChangeRows(
            ResultSet rowsResult, TableRef table, Long currentScn, SyncMode syncMode, Instant startTime)
            throws SQLException {
        boolean hasData = false;
        long scn = currentScn;
        TableChangeRow tableChangeRow = new TableChangeRow(rowsResult);
        boolean firstRow = true;
        while (rowsResult.next()) {
            if (firstRow) {
                metricsHelper.publishVersionQueryExecution(Duration.between(startTime, java.time.Instant.now()));
                firstRow = false;
            }
            scn = tableChangeRow.getVersionsStartScn();

            Map<String, Object> row = buildFlashbackRow(table, rowsResult);

            doForEachChange(tableChangeRow, table, row, syncMode);
            hasData = true;
        }
        saveVersionQueryExtractionMetrics(Duration.between(startTime, java.time.Instant.now()));
        if (isFirstRun || hasData) {
            // update wasTableUpdated. We clear wasTableUpdated only during the first run.
            state.setWasTableUpdated(table, hasData);
        }
        return scn;
    }

    public void forEachChange(
            TableRef tableRef,
            String rowId,
            Map<String, Object> row,
            ChangeType type,
            Instant timeOfChange,
            Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys,
            Map<TableRef, List<OracleColumn>> selected) {
        List<Column> targetColumns = outputHelper.getColumns(tableRef);
        Map<String, Object> exampleRow = new HashMap<>(row);
        List<String> dateCols = columnNamesWithDateType.get(tableRef);

        // this for-block is for coercing types for LogMiner (for which all values come through as strings)
        // Flashback columns pass through with no effect
        for (int i = 0; i < targetColumns.size(); i++) {
            Column col = targetColumns.get(i);
            if (!(exampleRow.get(col.name) instanceof String)) {
                // it is either null, which is fine, or it is already a coerced type
                continue;
            }

            String rawValue = (String) exampleRow.get(col.name);
            OracleColumn oracleColumn = selected.get(tableRef).get(i);

            if (!FlagName.OracleExplicitTypeForNumbersAndDates.check() && oracleColumn.oracleType.isNumber()) {
                // pass the number through BigDecimal to get a more uniform output
                Object minimalNumberType = OracleOutputHelper.minimizeType(new BigDecimal(rawValue));
                exampleRow.put(col.name, minimalNumberType);
            } // TODO: Handle numeric case when on OracleExplicitTypeForNumbersAndDates FF

            switch (col.type) {
                case Unknown:
                    if (dateCols.contains(col.name)) {
                        // Force the string into LocalDate, LocalDateTime or Instant
//                        exampleRow.put(col.name, OracleDate.anyFromString(rawValue, oracleColumn))
                    }
                    break;
                case Instant:
                    exampleRow.put(col.name, OracleType.instantFromString(rawValue, oracleColumn));
                    break;
                case LocalDate:
                    try {
                        exampleRow.put(col.name, OracleType.localDateFromString(rawValue, oracleColumn));
                    } catch (TypePromotionNeededException e) {
                        outputHelper.promoteColumn(tableRef, oracleColumn, DataType.LocalDateTime);
                        exampleRow.put(col.name, OracleType.localDateTimeFromString(rawValue, oracleColumn));
                    }
                    break;
                case LocalDateTime:
                    exampleRow.put(col.name, OracleType.localDateTimeFromString(rawValue, oracleColumn));
                    break;
                case Binary:
                    exampleRow.put(col.name, BinaryUtil.hexStringToByteArray(rawValue));
                    break;
                default:
                    // Do nothing: leave type coercing up to core
            }
        }

        List<Column> exampleColumns =
                targetColumns.stream().filter(t -> row.containsKey(t.name)).collect(Collectors.toList());

        if (pKeylessTables.contains(tableRef)) {
//            exampleColumns.add(Column.String(Names.example_ID_COLUMN).primaryKey(true).build());

            if (partitionedTablesWithRowMovementEnabled.contains(tableRef)) {
                // Logminer should have values for all columns of the row so that we can correctly calculate
                // the hashId. This means EITHER the table has no unique constraints AND no non-null unique
                // indices OR it has supplemental logging enabled for "ALL" columns.
                if (targetColumns.size() != row.size()) {
                    String message =
                            String.format(
                                    "Expected number of changed columns to match number of incoming columns. table: %s rowid: %s",
                                    tableRef, rowId);
                    throw new IllegalStateException(message);
                }

//                exampleRow.put(Names.example_ID_COLUMN, hashIdGenerator.hashId(row, targetColumns));

            } else {
//                exampleRow.put(Names.example_ID_COLUMN, rowId);
            }
        }
        writeData(
                selected,
                tableRef,
                type,
                timeOfChange,
                exampleRow,
                exampleColumns,
                row,
                maybeRowPrimaryKeys,
                targetColumns);
    }

    @Override
    public void stop() throws OracleLogStreamException {
        // Nothing to do
    }

    @Override
    public void close() {
        LOG.info(
                "SCN to Timestamp cache stats: hits="
                        + scnToTimestampCache.getHitCount()
                        + " misses="
                        + scnToTimestampCache.getMissCount());
    }

    private void writeData(
            Map<TableRef, List<OracleColumn>> selected,
            TableRef table,
            ChangeType type,
            Instant timeOfChange,
            Map<String, Object> exampleRow,
            List<Column> exampleColumns,
            Map<String, Object> row,
            Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys,
            Collection<Column> targetColumns) {
        if (selected.containsKey(table)) {
            if (pKeylessTables.contains(table) && partitionedTablesWithRowMovementEnabled.contains(table)) {
                // Use append-only strategy just for pkeyless partitioned tables with row movement enabled
                outputHelper.submitRecord(exampleRow, table, exampleColumns, timeOfChange, INSERT);

            } else {
                if (type == INSERT || type == DELETE) {
                    outputHelper.submitRecord(exampleRow, table, exampleColumns, timeOfChange, type);
                } else if (type == UPDATE) {
                    if (maybeRowPrimaryKeys.isPresent() && maybeRowPrimaryKeys.get().changed()) {
                        // the primary key changed, we need to do a delete, then upsert
                        Transactions.RowPrimaryKeys rowPrimaryKeys = maybeRowPrimaryKeys.get();

                        // this means they haven't done the supplemental logging change needed for LogMiner pKey
                        // changes.
                        // This warning gives them instructions
                        if (targetColumns.size() != row.size()) {
                            LOG.warning("PrimaryKeyChangeWarning {table='" + table + "', " + rowPrimaryKeys + "}");
//                            outputHelper.warn(new PrimaryKeyChangeWarning(table));
                            return;
                        }

                        // build a delete row with the old primary key(s)
                        Map<String, Object> deleteRow = new HashMap<>();
                        List<Column> deletedColumns = new ArrayList<>();
                        rowPrimaryKeys.primaryKeyColumns.forEach(
                                c -> {
                                    deleteRow.put(c.name, rowPrimaryKeys.getOldValue(c.name));
                                    deletedColumns.add(c.asexampleColumn(table.schema));
                                });

                        // submit delete
                        outputHelper.submitRecord(deleteRow, table, deletedColumns, timeOfChange, DELETE);
                        // submit insert
                        outputHelper.submitRecord(exampleRow, table, exampleColumns, timeOfChange, INSERT);
                    } else if (row.size() == targetColumns.size()) {
                        // We need to do this in case we skipped an insert with invalid rowid and we now
                        // have the
                        // update operation that goes with the skipped insert. This update operation
                        // will have full
                        // row data as well
                        outputHelper.submitRecord(exampleRow, table, exampleColumns, timeOfChange, INSERT);
                    } else {
                        outputHelper.submitRecord(exampleRow, table, exampleColumns, timeOfChange, UPDATE);
                    }
                }
            }
        }
    }

    Map<String, Object> buildFlashbackRow(TableRef tableRef, ResultSet rowsResult) {
        Iterable<OracleColumn> columns = includedTables.get(tableRef);
        Map<String, Object> flashbackRow = new HashMap<>();
        for (OracleColumn col : columns) {
            Object value = outputHelper.coerce(tableRef, rowsResult, col);
            flashbackRow.put(col.name, value);
        }
        return flashbackRow;
    }

    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void resync(TableRef tableRef, String reason, Boolean logEventNow) {
        resyncHelper.addToResyncSet(tableRef, reason, logEventNow);
        state.resetTable(tableRef);
    }

    static List<Column> asOutputColumns(TableRef table, Collection<OracleColumn> columns) {
        return columns.stream().map(c -> c.asexampleColumn(table.schema)).collect(Collectors.toList());
    }

    static List<String> retrieveColumnNamesWithDateType(TableRef table, Collection<OracleColumn> columns) {
        return columns.stream().filter(c -> c.oracleType.isDate()).map(c -> c.name).collect(Collectors.toList());
    }

    static Set<TableRef> primaryKeylessTables(Map<TableRef, List<OracleColumn>> selected) {
        return selected.entrySet()
                .stream()
                .filter(entry -> entry.getValue().stream().noneMatch(c -> c.primaryKey))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public void doIncrementalWork() throws OracleLogStreamException {
        flashbackUpdate();
    }

    /**
     * Returns cached Instant rerpesenting closest timestamp associated with scn or calls source database to perform the
     * conversion.
     *
     * @param scn The SCN for which we want to find the Timestamp
     * @return Instant representing timestamp associated with
     */
    public Instant scnToTimestamp(Long scn) {
        return api.convertScnToTimestamp(scn);
    }
}