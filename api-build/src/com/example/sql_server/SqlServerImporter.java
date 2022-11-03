package com.example.sql_server;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbImporter;
import com.example.db.DbRowSize;
import com.example.db.SqlStatementUtils;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.logger.event.integration.ReadEvent;
import com.example.micrometer.TagValue;
import com.example.oracle.Names;
import com.example.utils.ExampleClock;
import io.micrometer.core.instrument.LongTaskTimer;
import io.opentelemetry.extension.annotations.WithSpan;
import org.apache.commons.codec.binary.Hex;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class SqlServerImporter extends SqlServerSyncer implements DbImporter<TableRef> {

    private Duration extractImportDuration = Duration.ofSeconds(0);
    //    LongTaskTimer importTimer =
//            exampleConnectorMetrics.get()
//                    .longTaskTimer(MetricDefinition.SYNC_DURATION, TagName.SYNC_PHASE.toString(), "import");
    LongTaskTimer importTimer = null;
    /*
     * Page size is what we put in the LIMIT clause of the SQL query. It determines the size of the result set.
     * There needs to be a limit because otherwise the query will literally take forever on large tables, and can
     * potentially blow up the customer's server. If you set the limit too low, it will take forever to page through
     * a large table.
     */
    public static final long DEFAULT_PAGE_SIZE = 5_000_000L;
    public static final long INDEXED_TABLE_PAGE_SIZE = 500_000L;

//    public SqlServerImporter() {
//        super(null, null, null, null);
//    }

    public SqlServerImporter(
            SqlServerSource sqlServerSource,
            SqlServerState sqlServerState,
            SqlServerInformer sqlServerInformer,
            Output<SqlServerState> out) {
        super(sqlServerSource, sqlServerState, sqlServerInformer, out);
    }

    @Override
    public Comparator<TableRef> tableSorter() {
        return Comparator.comparingLong(t -> informer.tableInfo(t).estimatedDataBytes.orElse(0L));
    }

    @Override
    public boolean importStarted(TableRef tableRef) {
        return state.tables.containsKey(tableRef) && state.tables.get(tableRef).importStarted();
    }

    @Override
    public boolean importFinished(TableRef tableRef) {
        return state.isImportFinished(tableRef);
    }

    //    @Override
    public void softDelete(TableRef tableRef) {
        // TODO move this higher up in the interfaces after removing TableRef as type parameter
        // (https://github.com/example/engineering/pull/86313)
        if (informer.syncModes().get(tableRef) != SyncMode.Legacy) return;

        SqlServerTableInfo tableInfo = informer.tableInfo(tableRef);
        Instant importBeginTime =
                Optional.ofNullable(state.tables.get(tableRef))
                        .map(tableState -> tableState.importBeginTime)
                        .orElseThrow(() -> new RuntimeException("Import begin time was not set"));

        out.softDelete(
                tableInfo.destinationTableRef,
                Names.example_DELETED_COLUMN,
                Names.example_SYNCED_COLUMN,
                importBeginTime);

        state.setTableWasSoftDeleted(tableRef, true);

        out.checkpoint(state);
    }

    @Override
    public boolean softDeletedAfterImport(TableRef sourceTableRef) {
        return Optional.ofNullable(state.tables.get(sourceTableRef))
                .map(tableState -> tableState.wasSoftDeleted)
                .orElse(false);
    }

    @Override
    @WithSpan
    public void importPage(TableRef tableRef) {
//        LongTaskTimer.Sample timerSample = importTimer.start();
        if (FlagName.SqlServerOrderByPK.check()) {
            Instant importBeginTime = state.tables.get(tableRef).importBeginTime;
            if (importBeginTime.isAfter(Instant.parse("2021-03-03T12:21:01Z"))
                    && importBeginTime.isBefore(Instant.parse("2021-03-19T12:00:00Z"))) {
                LOG.info(tableRef + " needs resync because it was imported in buggy order by pk window");
                if (FeatureFlag.check("SqlServerOrderByPKBugResync")) {
//                    LOG.customerWarning(
//                            ForcedResyncEvent.ofTable(
//                                    tableRef.schema,
//                                    tableRef.name,
//                                    ForcedResyncEvent.ResyncReason.BUG_FIX,
//                                    "Bug introduced in order by primary key implementation"));
                    state.resetTableState(tableRef);
                }
            }

            if (!state.getLastKey(tableRef).isPresent()) {
                state.importWithOrderByPk.add(tableRef);
            }
        }

        Instant start = ExampleClock.Instant.now();
        LOG.info(ReadEvent.start(tableRef.toString()));

        SqlServerTableInfo tableInfo = informer.tableInfo(tableRef);
        long bytesOfDataBeforePage = this.extractVol;

        source.executeWithRetry(
                connection -> {
                    Optional<Long> pageSize = getPageSize(tableInfo);
                    Optional<List<String>> latestEvaluatedKey;

                    try (PreparedStatement selectRowStatement = selectRows(connection, tableInfo, pageSize);
                         ResultSet rows = selectRowStatement.executeQuery()) {
                        latestEvaluatedKey = processRows(rows, tableInfo, pageSize);
                        logExtractVol(tableRef.toString(), (this.extractVol - bytesOfDataBeforePage));
                    }

                    if (latestEvaluatedKey.isPresent()) {
                        state.setImportProgressV3(tableRef, latestEvaluatedKey);
                    } else {
                        state.setImportFinishedV3(tableRef);
                    }
                });

        out.checkpoint(state);
        extractImportDuration = extractImportDuration.plus(Duration.between(start, ExampleClock.Instant.now()));
//        timerSample.stop();
    }

    @WithSpan
    public PreparedStatement selectRows(Connection c, SqlServerTableInfo tableInfo, Optional<Long> pageSize)
            throws SQLException {
        String page = pageSize.map(size -> "TOP " + size).orElse("");
        String columns =
                tableInfo
                        .includedColumnInfo()
                        .stream()
                        .filter(col -> col.sourceType.isSupported)
                        .map(SqlServerSyncer::wrap)
                        .collect(Collectors.joining(", "));
        String pageClause =
                pageSize.map(
                        __ -> {
                            String keyColumns = null;
                            // For now we need to maintain the existing import to run properly
                            if (FlagName.SqlServerOrderByPK.check()) {
                                if (state.importWithOrderByPk.contains(tableInfo.sourceTable)) {
                                            keyColumns =
                                                    SqlStatementUtils.SQL_SERVER.columnsForOrderBy(
                                                            tableInfo.primaryKeys());
                                } else {
                                    keyColumns =
                                            tableInfo
                                                    .primaryKeys()
                                                    .stream()
                                                    .sorted(Comparator.comparingInt(a -> a.ordinalPosition))
                                                    .map(
                                                            ci ->
                                                                    SqlStatementUtils.SQL_SERVER.quote(
                                                                            ci.columnName))
                                                    .collect(Collectors.joining(", "));
                                }
                            } else {
//                                        keyColumns =
                                SqlStatementUtils.SQL_SERVER.columnsForOrderBy(tableInfo.primaryKeys());
                            }

                            List<SqlServerColumnInfo> allKeyColumnInfo =
                                    new ArrayList<>(tableInfo.primaryKeys());
                            allKeyColumnInfo.sort(
                                    Comparator.comparing(
                                            pk ->
                                                    state.importWithOrderByPk.contains(tableInfo.sourceTable)
                                                            ? pk.primaryKeyOrdinalPosition
                                                            : pk.ordinalPosition));
                            Optional<String> whereGreaterThanLastKey =
                                    state.getLastKey(tableInfo.sourceTable)
                                            .map(
                                                    lk ->
                                                            SqlStatementUtils.SQL_SERVER.whereGreaterThan(
                                                                    allKeyColumnInfo, lk));

                            return (whereGreaterThanLastKey.map(s -> "WHERE (" + s + ")").orElse(""))
                                    + (keyColumns != null ? (" ORDER BY " + keyColumns) : "");
                        })
                        .orElse("");

        return c.prepareStatement(
                "SELECT "
                        + page
                        + " "
                        + columns
                        + " FROM "
                        + SqlStatementUtils.SQL_SERVER.quote(tableInfo.sourceTable)
                        + " "
                        + pageClause);
    }

    @WithSpan
    public Optional<List<String>> processRows(ResultSet rows, SqlServerTableInfo tableInfo, Optional<Long> pageSize)
            throws SQLException {
        Map<String, Object> values = new HashMap<>();

        long recordCount = 0;
        boolean historySync = tableInfo.tableDefinition().isHistoryModeWrite;

        while (rows.next()) {
            values = extractRowValues(tableInfo, tableInfo.includedColumnInfo(), sourceValueFetcher(rows), false);
            if (historySync) out.upsert(tableInfo.tableDefinition(), values, ExampleClock.Instant.now());
            else out.upsert(tableInfo.tableDefinition(), values);
            recordCount++;
            processDataVol(values);
        }

//        LOG.customerInfo(ProcessedRecordsEvent.of(tableInfo.sourceTable.toString(), recordCount));

        return extractLastEvaluatedKey(tableInfo, pageSize, recordCount, values);
    }

    private void processDataVol(Map<String, Object> values) {
        long rowSize = DbRowSize.mapValues(values);
//        byteCounter.increment(rowSize);
//        rowCounter.increment();
        extractVol += rowSize;
    }

    /**
     * Get last value of primary key to be used as the paging cursor. An empty list is returned for empty pages and for
     * tables without any primary key.
     */
    public Optional<List<String>> extractLastEvaluatedKey(
            SqlServerTableInfo tableInfo, Optional<Long> pageSize, long recordCount, Map<String, Object> values) {

        if (!pageSize.isPresent() || recordCount < pageSize.get()) {
            return Optional.empty();
        }

        List<String> lastKey;
        if (FlagName.SqlServerOrderByPK.check() && state.importWithOrderByPk.contains(tableInfo.sourceTable)) {
            List<SqlServerColumnInfo> pkeys = new ArrayList<>(tableInfo.primaryKeys());
            pkeys.sort(Comparator.comparing(a -> a.primaryKeyOrdinalPosition));

            lastKey =
                    pkeys.stream()
                            .peek(c -> assertValueNonNull(c.columnName, values, tableInfo.sourceTable))
                            .map(c -> stringify(values.get(c.columnName)))
                            .collect(Collectors.toList());
        } else {
            lastKey =
                    tableInfo
                            .sourceColumns()
                            .stream()
//                            .filter(c -> c.primaryKey)
                            .peek(c -> assertValueNonNull(c.name, values, tableInfo.sourceTable))
                            .map(c -> stringify(values.get(c.name)))
                            .collect(Collectors.toList());
        }
        return Optional.of(lastKey);
    }

    private static void assertValueNonNull(String columnName, Map<String, Object> values, TableRef tableRef) {
        if (values.get(columnName) == null) {
            throw new AssertionError("Primary key column " + columnName + " has NULL value for table " + tableRef);
        }
    }

    // TODO: Switch to using PreparedStatement methods to inject values into sql query
    public static String stringify(Object value) {

        // SQL Server expects the seconds resolution when making comparisons in the WHERE clause we
        // generate for import query.
        if (value instanceof LocalDateTime) {
            return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((LocalDateTime) value);
        }

        // Inputting binary values as hexadecimals in the WHERE clause
        if (value instanceof byte[]) {
            return "0x" + new String(Hex.encodeHex((byte[]) value, false));
        }

        return value.toString();
    }

    public Optional<Long> getPageSize(SqlServerTableInfo tableInfo) {
        if (tableInfo.hasPrimaryKey()) {
            Optional<Long> pageSize =
                    Optional.of(tableInfo.hasClusteredIndex ? INDEXED_TABLE_PAGE_SIZE : DEFAULT_PAGE_SIZE);
            if (FeatureFlag.check("SmallerSqlServerPageSizeForPeopleWhoManageDatabasesPoorly"))
                pageSize = Optional.of(INDEXED_TABLE_PAGE_SIZE);
            return pageSize;
        }

        return Optional.empty();
    }

    @Override
    public void recordImportDuration(TagValue.SyncStatus syncStatus, String dbVersion) {
        if (!(extractImportDuration.isZero() || extractImportDuration.isNegative())) {
            // using `simpleTimer` since we are not pre calculate percentiles
//            METRICS.simpleTimer(
//                            MetricDefinition.SYNC_EXTRACT_IMPORT_DURATION,
//                            TagName.SYNC_STATUS.toString(),
//                            syncStatus.value,
//                            TagName.DB_VERSION.toString(),
//                            dbVersion)
//                    .record(extractImportDuration);
            LOG.info(String.format("Import duration %d ms sent to NewRelic", extractImportDuration.toMillis()));
        }
    }
}
