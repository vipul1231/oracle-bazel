package com.example.oracle.logminer;

import com.example.core.TableRef;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.logger.event.integration.InfoEvent;
import com.example.logger.event.integration.WarningEvent;
import com.example.oracle.*;
import com.example.oracle.exceptions.InvalidScnException;
import com.example.utils.ExampleClock;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;

import static com.example.oracle.Constants.*;
import static com.example.oracle.Constants.MAX_RETRIES;
import static com.example.oracle.LogMinerOperation.*;
import static com.example.oracle.LogMinerOperation.DDL_OP;
import static com.example.oracle.Util.binaryXidToString;
import static com.example.oracle.Util.doubleQuote;
import static java.lang.String.format;

/**
 * Oracle Logminer incremental update related functions
 *
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/8/2021<br/>
 * Time: 10:24 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleLogMinerIncrementalUpdater {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    final OracleApi api;


    private Queue<LogMinerEventInfo> missingTransactions = new LinkedList<>();
    // To hold the transactions not found in current page, but available in future pages
    private Map<String, LogMinerOperation> lmTransactions = new HashMap<>();

    public OracleLogMinerIncrementalUpdater(OracleApi api) {
        this.api = api;
    }

    /**
     * Get all committed changes from database, starting at `start`
     *
     * @param start           System-change-number of the earliest change that we might not have synced to the warehouse
     * @param selected        Tables included for sync'ing by the user and their columns
     * @param invalidateTable LogMiner has encountered an event for this table that it cannot read, the table needs to
     *                        be reimported
     * @param forEach         What to do with each change
     * @return The SCN of the earliest change that we haven't synced to the warehouse
     */
    public Transactions.ChangedRowsResult changedRows(
            long start,
            Optional<Long> previousPageSize,
            Map<TableRef, List<OracleColumn>> selected,
            BiConsumer<TableRef, String> invalidateTable,
            Consumer<TableRef> softDeleteTable,
            ForEachChange forEach,
            Consumer<Long> afterPageSuccess)
            throws InvalidScnException {

        long end = api.mostRecentScn();
        if (start >= end) {
            LOG.info("There are no incremental changes (no new archive logs)");
//            return new ChangedRowsResult(start, previousPageSize.orElse(pageSizeConfig.get().getStart()));
        }

        return processRowChunks(
                start,
                end,
                (c, s, e, m) -> changedRowsChunk(c, s, e, m, selected, invalidateTable, softDeleteTable, forEach),
                afterPageSuccess,
                previousPageSize);
    }

    public Transactions.ChangedRowsResult processRowChunks(
            long start,
            long end,
            Transactions.LogChunkProcessor processor,
            Consumer<Long> afterPageSuccess,
            Optional<Long> previousPageSize) {

        double allowableGap = 0.05;
        long closeToEnd = ((long) ((end - start) * (1 - allowableGap))) + start;
//        long pageSize = previousPageSize.orElse(pageSizeConfig.get().getStart());
        long pageSize = -1;

        LOG.info(
                "Total SCN range: "
                        + start
                        + "api/src/main "
                        + end
                        + " (failsafe: "
                        + closeToEnd
                        + "), which is "
                        + (end - start)
                        + " SCNs total");
//        LOG.info("Using SCN Config: " + pageSizeConfig.get());

        // We need `tempExtraPagesize` for cases where the current page size does not allow for progress to be made.
        // In Oracle, we can hit situations where the current page includes an uncommitted transaction, and the
        // commit / rollback for the txn is in a future page.
        // In these cases we need to grow the page size, but only temporarily. By keeping track of the temporary
        // increase with `tempExtraPagesize`, we don't pollute the new page size calculation at the end of each loop.
        long tempExtraPagesize = 0;
        long lastNanosPerScn = Long.MAX_VALUE;
        long syncStartScn = start;

        while (true) { // infinite loop potential

            Transactions.ProcessRowChunkPageResult result;

            result = processRowChunkPage(start, end, processor, pageSize, tempExtraPagesize, start < closeToEnd);

            if (result.actualEndScn == start) {

//                tempExtraPagesize += pageSizeConfig.get().getTempExtraStep();
                tempExtraPagesize += 0;

                if (result.plannedEndScn >= closeToEnd
                        && !FlagName.OracleProcessUncommittedTransactionsCommittedLater.check()) {
                    // no progress made && close to end of window
                    LOG.info(
                            format(
                                    "No row progress made, (%d -> %d ~= %s). Window covers most of the range: giving up.",
                                    start, end, result.pageSize));
                    return new Transactions.ChangedRowsResult(result.actualEndScn, pageSize);
                }
            } else {
                afterPageSuccess.accept(result.actualEndScn);

                if (!result.pageSize.isPresent()) {
                    throw new RuntimeException("pageSize is missing but required");
                }
                if (!result.nanosPerScn.isPresent()) {
                    throw new RuntimeException("nanosPerScn is missing but required");
                }

                // use adjusted pageSize if provided
                pageSize = result.pageSize.orElse(pageSize);

                if (result.nanosPerScn.get() > lastNanosPerScn) { // slower
//                    pageSize = Math.max(pageSizeConfig.get().getMin(), pageSize - pageSizeConfig.get().getStep());
                    pageSize = 0;
                } else if (result.actualEndScn - start >= pageSize) { // faster + did at least full page
//                    pageSize += pageSizeConfig.get().getStep();
                    pageSize += 0;
                }

                lastNanosPerScn = result.nanosPerScn.get();
                tempExtraPagesize = 0;
            }

            start = result.actualEndScn;

            if (FlagName.OracleProcessUncommittedTransactionsCommittedLater.check() && !missingTransactions.isEmpty()) {

                LogMinerEventInfo lmEventInfo;
                while ((lmEventInfo = missingTransactions.poll()) != null) {

                    Optional<LogMinerOperation> lmOperation = api.getLatestTransactionStatus(lmEventInfo.xid, start);
                    if (lmOperation.isPresent()) {
                        LOG.info(
                                "Adding missing transaction status for xId:"
                                        + lmEventInfo.xid
                                        + " opCode:"
                                        + lmOperation.get());
                        lmTransactions.put(lmEventInfo.xid, lmOperation.get());
                    } else {
                        LOG.customerWarning(
                                WarningEvent.warning(
                                        "uncommitted_txn_alert",
                                        "Encountered incomplete transaction SCN: "
                                                + lmEventInfo.scn
                                                + " for transaction ID: "
                                                + lmEventInfo.xid
                                                + ". The connector cannot proceed until this transaction is committed."));

                        // if this cycle start SCN is an uncommitted transactions and its pending for more than
                        // UPPER_BOUND_INTERVAL
                        if (result.actualEndScn == syncStartScn
                                && ExampleClock.Instant.now()
                                .isAfter(
                                        api.convertScnToTimestamp(lmEventInfo.scn)
                                                .plus(upperBoundIntervalForUncommittedTransaction))) {

//                            throw CompleteWithTask.create(
//                                    new UncommittedTransactionBlockingSync(
//                                            lmEventInfo.scn,
//                                            lmEventInfo.status,
//                                            lmEventInfo.operationCode,
//                                            lmEventInfo.segOwner,
//                                            lmEventInfo.tableName,
//                                            lmEventInfo.xid,
//                                            lmEventInfo.rowId),
//                                    new RuntimeException("Uncommitted Transaction Blocking Sync"));
                        } else {
                            LOG.info("Quitting incremental sync due to uncommitted transaction.");
                            return new Transactions.ChangedRowsResult(result.actualEndScn, pageSize);
                        }
                    }
                }
            }

            if (result.actualEndScn >= end) {
                // all caught up
                return new Transactions.ChangedRowsResult(result.actualEndScn, pageSize);
            }
        }
    }

    public Transactions.ProcessRowChunkPageResult processRowChunkPage(
            long start,
            long end,
            Transactions.LogChunkProcessor processor,
            long pageSize,
            long tempExtraPagesize,
            boolean morePage) {

        // Not using `retry()` here since we need to implement custom failure logic. `retry()` does not make that
        // easy.
        for (int attempt = 1; /* see catch */ ; attempt++) {
            try (Connection connection = api.connectToSourceDb()) {

                long plannedEndScn = Math.min(end, start + pageSize + tempExtraPagesize);

                long startTime = System.nanoTime();
                long actualEndScn =
                        processor.processChunk(connection, start, plannedEndScn, morePage && plannedEndScn < end);
                long nanosPerScn = (System.nanoTime() - startTime) / (pageSize + tempExtraPagesize);

                return new Transactions.ProcessRowChunkPageResult(plannedEndScn, actualEndScn, pageSize, nanosPerScn);
            } catch (SQLDataException e) {
                // these are non-recoverable exceptions. Usually related to bad data in a query like dividing by
                // zero
                // see: https://docs.oracle.com/javase/8/docs/api/index.html?java/sql/SQLDataException.html
                throw new RuntimeException(
                        "Failed to perform incremental update on all tables after " + attempt + " attempts", e);
            } catch (SQLRecoverableException e) {
                if (attempt >= MAX_PAGE_SIZE_SHRINK_RETRIES) {
                    throw new RuntimeException(
                            "Failed to perform incremental update on all tables after " + attempt + " attempts", e);
                }
                // try again, shrink page!
                LOG.log(Level.WARNING, "Failed to perform incremental update, shrinking page and trying again", e);
                pageSize = pageSize / attempt;
            } catch (SQLException e) {
                if (attempt >= MAX_RETRIES) {
                    if (e.getMessage().contains("ORA-01291: missing logfile")
                            || e.getMessage()
                            .contains(
                                    "ORA-01292: no log file has been specified for the current LogMiner session")) {
                        throw new InvalidScnException("Fell behind processing incremental logs", e);
                    }

                    throw new RuntimeException(
                            "Failed to perform incremental update on all tables after " + attempt + " attempts", e);
                }

                Transactions.sleepForRetry(Transactions.sleepDurationFunction, attempt, e, "incremental update on all tables");
            }
        }
    }

    static class LogMinerEventInfo {
        final long scn;
        final int status;
        final int operationCode;
        final String segOwner;
        final String tableName;
        final String xid;
        final String rowId;

        public LogMinerEventInfo(
                long scn, int status, int operationCode, String segOwner, String tableName, String xid, String rowId) {
            this.scn = scn;
            this.status = status;
            this.operationCode = operationCode;
            this.segOwner = segOwner;
            this.tableName = tableName;
            this.xid = xid;
            this.rowId = rowId;
        }
    }

    private long changedRowsChunk(
            Connection connection,
            long start,
            long end,
            boolean more,
            Map<TableRef, List<OracleColumn>> selected,
            BiConsumer<TableRef, String> invalidateTable,
            Consumer<TableRef> softDeleteTable,
            ForEachChange forEach)
            throws SQLException {

        LOG.info(
                "Processing incremental changes for SCN: "
                        + start
                        + "api/src/main "
                        + end
                        + ", which is "
                        + (end - start)
                        + " SCNs total");
        try (LogMinerSession logMinerSession = new LogMinerSession(connection).advancedOptions().scnSpan(start, end)) {
            logMinerSession.start();

            SQLSupplier<Map<String, LogMinerOperation>> completed = () -> logMinerSession.getLogMinerContentsDao().completedTransactions(connection);

            Set<TableRef> modifiedTables = logMinerSession.getLogMinerContentsDao().getModifiedTables(connection, selected.keySet());
            if (modifiedTables.isEmpty()) {
                LOG.info("There are no incremental changes for selected tables");
                // return end + 1 so we can move to the next page
                return end + 1;
            }

//            Map<TableRef, List<OracleColumn>> targetedTables = Maps.filterKeys(selected, modifiedTables::contains);
            Map<TableRef, List<OracleColumn>> targetedTables = null;

            Map<TableRef, Integer> targetedTablesPKCountCache = new HashMap<>();
            Function<TableRef, Integer> targetedTablesPKCount =
                    tableRef ->
                            targetedTablesPKCountCache.computeIfAbsent(
                                    tableRef,
                                    t -> (int) targetedTables.get(tableRef).stream().filter(c -> c.primaryKey).count());

            try (PreparedStatement preparedStatement = selectChangedRows(connection, targetedTables);
                 ResultSet rows = preparedStatement.executeQuery()) {

                LOG.info("Writing incremental changes");

                return writeCommittedRows(
                        rows,
                        completed,
                        selected.keySet(),
                        invalidateTable,
                        softDeleteTable,
                        start,
                        end,
                        more,
                        (op, tableRef, rowId) -> {
                            if (!targetedTables.containsKey(tableRef)) {
                                return;
                            }

                            // Skip DML with invalid rowid. So far we came across two cases of
                            // these:
                            // #1 Inserts involving tables with columns of CLOB data type:
                            //     This kind of insert is logged as insert with invalid rowid
                            //     (AAAAAAAAAAAAAAAAAA) + update with valid rowid
                            // #2 Certain update operations on tables with a non-null unique index:
                            //     These updates don't change any values of the row. They set the
                            //     value of the unique index column to the same value and don't use
                            //     any other column
                            // TODO: Figure out how to reproduce #2 and write a test for it
                            if (api.isInvalidRowId(rowId)) {
                                return;
                            }

                            ChangeType changeType = ChangeType.changeType(op);

                            List<OracleColumn> columns = targetedTables.get(tableRef);
                            Map<String, Object> row = api.buildRow(columns, rows::getString);

                            Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys = Optional.empty();

                            if (changeType == ChangeType.UPDATE && targetedTablesPKCount.apply(tableRef) > 0) {
                                maybeRowPrimaryKeys = Optional.of(Transactions.RowPrimaryKeys.build(columns, rows::getString, row));
                            }

                            forEach.accept(tableRef, rowId, row, changeType, maybeRowPrimaryKeys);
                        });
            }
        }
    }

    // TODO make this return String and wrap the query string in PreparedStatement outside
    @SuppressWarnings("java:S2095")
    private PreparedStatement selectChangedRows(Connection connection, Map<TableRef, List<OracleColumn>> targetedTables)
            throws SQLException {

        LOG.info("Building query to select changed rows");

        Map<String, String> caseColumns = new LinkedHashMap<>();

        /*
        SELECT SCN, STATUS, OPERATION_CODE, SEG_OWNER, TABLE_NAME, XID, ROW_ID,

        -- Column 1 of tables --
        CASE
        WHEN SEG_OWNER = 'DEVELOPERS' AND TABLE_NAME = 'animals' THEN
           CASE
           WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE, '"DEVELOPERS"."animals"."id"')=1
               THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '"DEVELOPERS"."animals"."id"' ))
           WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE, '"DEVELOPERS"."animals"."id"')=1
               THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '"DEVELOPERS"."animals"."id"' ))
           ELSE '_SKIP_'
           END

        WHEN SEG_OWNER = 'DEVELOPERS' AND TABLE_NAME = 'planes' THEN
           CASE
           WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE, '"DEVELOPERS"."planes"."price"')=1
               THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '"DEVELOPERS"."planes"."price"'))
           WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE, '"DEVELOPERS"."planes"."price"')=1
               THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '"DEVELOPERS"."planes"."price"' ))
           ELSE '_SKIP_'
           END

        WHEN ... more tables ... THEN
           CASE
           WHEN THEN ...
           WHEN THEN ...
           ELSE '_SKIP_'
           END

        ELSE ''
        END "c0",


        -- Column 2 of tables
        CASE
        WHEN table 1 THEN
           ...
        WHEN table 2 THEN
           ...
        WHEN ... only tables with 2 columns ...
           ...
        ELSE ''
        END "c1",

        ... more columns (up to max number of columns in any table) ...
        CASE
        END "c<N>"

        FROM SYS.V_$LOGMNR_CONTENTS

        WHERE (SEG_OWNER NOT IN (<SYSTEM_SCHEMAS>) OR SEG_OWNER IS NULL)
        --OR--
        WHERE SEG_OWNER IN (<modified_schemas>) AND TABLE_NAME IN (<modified_tables>)

              AND OPERATION_CODE in (1, 2, 3, 5)
        */

        Set<String> whereSchemas = new HashSet<>();
        Set<String> whereTables = new HashSet<>();

        targetedTables.forEach(
                (tableRef, columns) -> {
                    String from = doubleQuote(tableRef);
                    whereSchemas.add("'" + tableRef.schema + "'");
                    whereTables.add("'" + tableRef.name + "'");

                    int pki = 0;
                    for (int i = 0; i < columns.size(); i++) {
                        OracleColumn col = columns.get(i);

                        String caseColName = "c" + i;
                        String fullColName = "'" + from + "." + doubleQuote(col.name) + "'";

                        String caseColVal = caseColumns.getOrDefault(caseColName, "");

                        String when =
                                "WHEN SEG_OWNER='"
                                        + tableRef.schema
                                        + "' AND TABLE_NAME='"
                                        + tableRef.name
                                        + "' "
                                        + "THEN "
                                        + "CASE "
                                        + "WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,"
                                        + fullColName
                                        + ")=1 "
                                        + "THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,"
                                        + fullColName
                                        + ")) "
                                        + "WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,"
                                        + fullColName
                                        + ")=1 "
                                        + "THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,"
                                        + fullColName
                                        + ")) "
                                        + "ELSE '"
                                        + SKIP_VALUE
                                        + "' "
                                        + "END ";

                        caseColumns.put(caseColName, caseColVal + when);

                        if (col.primaryKey) {
                            String caseOldPKName = "opk" + pki;
                            String caseOldPKVal = caseColumns.getOrDefault(caseOldPKName, "");
                            String oldPKWhen = getPrimaryKeyCaseWhen(tableRef, fullColName, Constants.LogDirection.UNDO_VALUE);
                            caseColumns.put(caseOldPKName, caseOldPKVal + oldPKWhen);
                            pki++;
                        }
                    }
                });

        for (String caseColName : caseColumns.keySet()) {
            String caseColVal = caseColumns.get(caseColName);
            caseColVal = "CASE " + caseColVal + " ELSE '' END " + doubleQuote(caseColName);
            caseColumns.put(caseColName, caseColVal);
        }

        caseColumns.put(
                "DDL_EVENT", "CASE WHEN OPERATION_CODE=5 THEN SQL_REDO ELSE '' END " + doubleQuote("DDL_EVENT"));

//        String allCaseCols = Joiner.on(",").join(caseColumns.values());
        String allCaseCols = null;

        // query runs faster this way compared to using multi-column IN clause with "modified" (schema, table)
        // pairs even though the query is less precise this way (i.e. it may return log entries for schemas and
        // tables we don't need; we'll filter them out later with java code)
//        @Language("SQL")
        String query =
                "SELECT SCN,STATUS,OPERATION_CODE,SEG_OWNER,TABLE_NAME,XID,ROW_ID,ROLLBACK,INFO,"
                        + allCaseCols
                        + " FROM SYS.V_$LOGMNR_CONTENTS"
                        + " WHERE SEG_OWNER IN ("
//                        + Joiner.on(',').join(whereSchemas)
                        + ") AND TABLE_NAME IN ("
//                        + Joiner.on(',').join(whereTables)
                        + ") "
                        + " AND OPERATION_CODE in (1,2,3,5)";

        LOG.info("Query to select changed rows ready with " + caseColumns.size() + " columns");

        PreparedStatement statement = connection.prepareStatement(query);
        statement.setFetchSize(DEFAULT_FETCH_SIZE);
        return statement;
    }

    private String getPrimaryKeyCaseWhen(TableRef tableRef, String fullColName, LogDirection dir) {
        return "WHEN SEG_OWNER='"
                + tableRef.schema
                + "' AND TABLE_NAME='"
                + tableRef.name
                + "' "
                + "THEN "
                + "CASE "
                + "WHEN OPERATION_CODE = 3 AND DBMS_LOGMNR.COLUMN_PRESENT("
                + dir
                + ","
                + fullColName
                + ")=1 "
                + "THEN (DBMS_LOGMNR.MINE_VALUE("
                + dir
                + ","
                + fullColName
                + ")) "
                + "ELSE '"
                + SKIP_VALUE
                + "' "
                + "END ";
    }

    /**
     * Writes committed rows until either all log entries are consumed or the first incomplete transaction is
     * encountered
     */
    public long writeCommittedRows(
            ResultSet rows,
            SQLSupplier<Map<String, LogMinerOperation>> completedTransactions,
            Set<TableRef> selected,
            BiConsumer<TableRef, String> invalidateTable,
            Consumer<TableRef> softDeleteTable,
            long start,
            long end,
            boolean more,
            Transactions.WriteRow row)
            throws SQLException {

        long rowCount = 0;
        long scn = INVALID_SCN;
        Set<TableRef> invalidatedTables = new HashSet<>();

        Map<String, LogMinerOperation> completed = completedTransactions.get();

        if (completed.isEmpty() && !FlagName.OracleProcessUncommittedTransactionsCommittedLater.check()) {
            LOG.info("There are no completed transactions");
            return start;
        }

        try {
            while (rows.next()) {
                String schema = rows.getString("SEG_OWNER");
                String table = rows.getString("TABLE_NAME");
                LogMinerOperation op = LogMinerOperation.valueOf(rows.getBigDecimal("OPERATION_CODE").intValue());

                if (op != DDL_OP && !selected.contains(new TableRef(schema, table))) {
                    continue;
                }

                scn = rows.getBigDecimal("SCN").longValue();
                String xid = binaryXidToString((byte[]) rows.getObject("XID"));

                // A transaction can be in one of 3 states:
                // 1.COMMIT  2.ROLLBACK  3.INCOMPLETE
                LogMinerOperation xidState;

                xidState = completed.getOrDefault(xid, INCOMPLETE_XID);

                if (FlagName.OracleProcessUncommittedTransactionsCommittedLater.check() && xidState == INCOMPLETE_XID) {
                    xidState = lmTransactions.getOrDefault(xid, INCOMPLETE_XID);
                    if (xidState != INCOMPLETE_XID)
                        LOG.info("Found transaction status from full SCN range for SCN:" + scn + " transaction:" + xid);
                }

                if (xidState == INCOMPLETE_XID && op != DDL_OP) {
                    // Hack fix for customers with uncommitted transactions that are rollback-only
                    int rollback = rows.getInt("ROLLBACK");
                    if (FeatureFlag.check("OracleSkipUncommittedRollbackOnlyTransactions") && rollback == 1) {
                        LOG.info("Skipping uncommitted rollback-only transaction ID " + xid);
                        continue;
                    }

                    if (FlagName.OracleProcessUncommittedTransactionsCommittedLater.check()) {
                        LOG.info(
                                "Encountered incomplete transaction for SCN: "
                                        + scn
                                        + ", transaction ID: "
                                        + xid
                                        + ". Adding transaction to queue");

                        int status = rows.getInt("STATUS");
                        int operationCode = rows.getBigDecimal("OPERATION_CODE").intValue();
                        String rowId = rows.getString("ROW_ID");
                        missingTransactions.add(
                                new LogMinerEventInfo(scn, status, operationCode, schema, table, xid, rowId));
                    } else {

                        if (!more) warnForUncommittedTransactions(rows);

                        LOG.customerWarning(
                                WarningEvent.warning(
                                        "uncommitted_txn_alert",
                                        "Encountered incomplete transaction SCN: "
                                                + scn
                                                + " for transaction ID: "
                                                + xid
                                                + ". The connector cannot proceed until this transaction is committed."));
                    }
                    // return where we are, we will start the next page with this scn
                    return scn;
                } else if (xidState == ROLLBACK_XID) {
                    // Skip DMLs belonging to rollback'ed transactions
                    continue;
                }

                if (op == DDL_OP) {
                    if (schema != null && table != null) {

                        TableRef tableRef = new TableRef(schema, table);

                        String ddlEvent = rows.getString("DDL_EVENT");
                        if (ddlEvent == null) {
                            ddlEvent = "NULL";
                        }

                        OracleDdlParser parser = new OracleDdlParser(ddlEvent);
                        Boolean shouldResyncDueToDdl = parser.shouldResync();

                        if (shouldResyncDueToDdl) {
                            String ddlMessage = "DDL detected: " + ddlEvent.trim();
                            LOG.info("DDL detected for table: " + tableRef + "  SCN: " + scn + " " + ddlMessage);

                            invalidateTable.accept(tableRef, ddlMessage);
                            invalidatedTables.add(tableRef);
                        } else if (parser.ddlType == DdlType.TRUNCATE) {
                            // trigger soft delete
                            softDeleteTable.accept(tableRef);
                            LOG.customerWarning(
                                    InfoEvent.of("TRUNCATE DDL", "Encountered TRUNCATE DDL for " + tableRef));
                        } else {
                            LOG.customerInfo(InfoEvent.of("DDL", "Ignoring DDL statement: " + ddlEvent.trim()));
                        }
                    }

                } else {
                    TableRef tableRef = new TableRef(schema, table);

                    if (invalidatedTables.contains(tableRef)) {
                        continue;
                    }

                    int status = rows.getInt("STATUS");
                    if (status != 0) {
                        /*
                         * This is what we call a DML event. At a high level, it means that the query in the SQL_REDO
                         * column is no longer a valid SQL statement. Most often this happens when the a DDL event has
                         * occurred for a table and the online dictionary of available columns / tables has changed.
                         * Since the dictionary has changed for this table we need to trigger a re-sync. In the future,
                         * there is probably a way to only trigger resync for come of these events (the column that
                         * changed might not be involved in this query), but for now we have to treat them all as
                         * unhandleable DDL events.
                         */
                        if (!invalidatedTables.contains(tableRef)) {
                            String info = rows.getString("INFO");
                            if (info == null) {
                                info = "NULL";
                            }
                            LOG.info("Invalid DML detected for table: " + tableRef + " Info: " + info.trim());
                            invalidateTable.accept(tableRef, "Invalid DML detected: " + info.trim());
                            invalidatedTables.add(tableRef);
                        }

                        continue;
                    }

                    String rowId = rows.getString("ROW_ID");
                    row.write(op, tableRef, rowId);

                    rowCount++;
                    if (rowCount % 50000 == 0) LOG.info("Wrote " + rowCount + " rows");
                }
            }
        } catch (SQLRecoverableException e) {
            /*
             * The theory here is that we can still save progress even if we run into a temporary network issue. If we
             * have made progress, we should return the progress. If we have not, we will throw the exception.
             */
            if (rowCount == 0) {
                LOG.warning("Caught a SQLRecoverableException, but we made NO progress; failing sync");
                throw e;
            }
            LOG.info("Caught a SQLRecoverableException, but we made progress; saving progress and continuing sync");
            return scn;
        } catch (NullPointerException e) {
            throw new RuntimeException("NullPointerException at scn " + scn, e);
        } finally {
            LOG.info("Imported " + rowCount + " rows for SCN range: " + start + "api/src/main " + scn);
        }

        if (scn == INVALID_SCN) {
            LOG.info("No rows were found to write");
        }

        return end + 1;
    }

    private void warnForUncommittedTransactions(ResultSet row) throws SQLException {
        long scn = row.getBigDecimal("SCN").longValue();
        int status = row.getInt("STATUS");
        int operationCode = row.getBigDecimal("OPERATION_CODE").intValue();
        String segOwner = row.getString("SEG_OWNER");
        String tableName = row.getString("TABLE_NAME");
        String xid = binaryXidToString((byte[]) row.getObject("XID"));
        String rowId = row.getString("ROW_ID");

        if (FeatureFlag.check("OracleResyncOn6HrUncommittedTxn")) {
            if (Duration.between(api.convertScnToTimestamp(scn), ExampleClock.Instant.now())
                    .compareTo(UPPER_BOUND_INTERVAL)
                    > 0) {
//                throw CompleteWithTask.create(
//                        new UncommittedTransactionBlockingSync(
//                                scn, status, operationCode, segOwner, tableName, xid, rowId),
//                        UncommittedTransactionBlockingSync.TYPE);
            } else if (Duration.between(api.convertScnToTimestamp(scn), ExampleClock.Instant.now())
                    .compareTo(INCOMPLETE_INTERVAL)
                    > 0) {
//                warningIssuer.issueWarning(
//                        new UncommittedWarning(scn, status, operationCode, segOwner, tableName, xid, rowId));
            }
        } else {
            if (Duration.between(api.convertScnToTimestamp(scn), ExampleClock.Instant.now())
                    .compareTo(UPPER_BOUND_INTERVAL)
                    > 0) {
//                warningIssuer.issueWarning(
//                        new UncommittedWarning(scn, status, operationCode, segOwner, tableName, xid, rowId));
            }
        }
    }
}