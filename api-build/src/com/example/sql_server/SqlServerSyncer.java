package com.example.sql_server;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.core2.Output;
import com.example.db.SqlStatementUtils;
import com.example.db.SyncerCommon;
import com.example.logger.ExampleLogger;
import com.example.oracle.Names;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.extension.annotations.WithSpan;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public abstract class SqlServerSyncer {

    static final ExampleLogger LOG = ExampleLogger.getMainLogger();

//    static final exampleMetrics METRICS = exampleConnectorMetrics.get();
//    protected Counter byteCounter = METRICS.counter(MetricDefinition.SYNC_EXTRACT_BYTE_COUNT);
//    protected Counter rowCounter = METRICS.counter(MetricDefinition.SYNC_EXTRACT_ROW_COUNT);

    final SqlServerSource source;
    final SqlServerState state;
    final SqlServerInformer informer;
    final Output<SqlServerState> out;
    final SyncerCommon syncerCommon;

//    private static final ObjectMapper JSON = DefaultObjectMapper.JSON;
    private static final ObjectMapper JSON = null;
    long extractVol = 0L;

    SqlServerSyncer(
            SqlServerSource sqlServerSource,
            SqlServerState sqlServerState,
            SqlServerInformer sqlServerInformer,
            Output<SqlServerState> out) {
        this.source = sqlServerSource;
        this.state = sqlServerState;
        this.informer = sqlServerInformer;
        this.out = out;
        this.syncerCommon = new SyncerCommon();
    }

    @FunctionalInterface
    interface SourceValueFetcher<Ex extends Exception> {
        Object fetch(SqlServerColumnInfo columnInfo) throws Ex;
    }

    <Ex extends Exception> Map<String, Object> extractRowValues(
            SqlServerTableInfo tableInfo, SourceValueFetcher<Ex> sourceValueFetcher, boolean deleted) throws Ex {
        return extractRowValues(tableInfo, tableInfo.includedColumnInfo(), sourceValueFetcher, deleted);
    }

    // TODO: Refactor method to not include deleted field
    @WithSpan
    <Ex extends Exception> Map<String, Object> extractRowValues(
            SqlServerTableInfo tableInfo,
            Iterable<SqlServerColumnInfo> selectedColumnsInTable,
            SourceValueFetcher<Ex> sourceValueFetcher,
            boolean deleted)
            throws Ex {

        Map<String, Object> values = new HashMap<>(tableInfo.totalDestinationColumns());
        for (SqlServerColumnInfo columnInfo : selectedColumnsInTable) {
            values.put(columnInfo.columnName, sourceValueFetcher.fetch(columnInfo));
        }

        syncerCommon.addexampleIdColumn(tableInfo, values);

        if (tableInfo.getSyncMode() == SyncMode.Legacy) values.put(Names.example_DELETED_COLUMN, deleted);

        return values;
    }

    static SourceValueFetcher<SQLException> sourceValueFetcher(ResultSet rows) {
        return columnInfo -> {
            DataType destType =
                    SqlServerType.destinationTypeOf(columnInfo.sourceType)
                            .orElseThrow(() -> new RuntimeException("Unsupported type " + columnInfo.sourceType));

            switch (destType) {
                case Boolean:
                    return rows.getObject(columnInfo.columnName, Boolean.class);
                case Short:
                    return rows.getObject(columnInfo.columnName, Short.class);
                case Int:
                    return rows.getObject(columnInfo.columnName, Integer.class);
                case Long:
                    return rows.getObject(columnInfo.columnName, Long.class);
                case Float:
                    return rows.getObject(columnInfo.columnName, Float.class);
                case Double:
                    return rows.getObject(columnInfo.columnName, Double.class);
                case BigDecimal:
                    return rows.getBigDecimal(columnInfo.columnName);
                case String:
                    return rows.getString(columnInfo.columnName);
                case Instant:
                    return SyncerCommon.convertColumnValue(
                            rows.getTimestamp(columnInfo.columnName), Timestamp::toInstant);
                case LocalDateTime:
                    return SyncerCommon.convertColumnValue(
                            rows.getTimestamp(columnInfo.columnName), Timestamp::toLocalDateTime);
                    // TODO: Handle date parsing exceptions gracefully
                case LocalDate:
                    return SyncerCommon.convertColumnValue(rows.getDate(columnInfo.columnName), Date::toLocalDate);
                case Binary:
                    return rows.getBytes(columnInfo.columnName);
                case Json:
                    switch (columnInfo.sourceType) {
                        case GEOMETRY:
                        case GEOGRAPHY:
                            /**
                             * TODO: Switch to using {@link com.microsoft.sqlserver.jdbc.Geometry#STAsBinary()} once
                             * https://github.com/microsoft/mssql-jdbc/issues/1280 is merged
                             */
                            return SyncerCommon.convertColumnValue(
                                    rows.getBytes(columnInfo.columnName), SqlServerSyncer::coerceSpatial);
                    }
                default:
                    throw new RuntimeException("Unsupported type " + columnInfo.sourceType);
            }
        };
    }

    void resyncWarning(TableRef table, String resyncMessage) {
//        LOG.customerWarning(
//                ForcedResyncEvent.ofTable(
//                        table.schema, table.name, ForcedResyncEvent.ResyncReason.STRATEGY, resyncMessage));

        // Displays UI warning for table resyncs
//        out.warn(new ResyncTableWarning(source.params.owner, table.schema, table.name, resyncMessage));
    }

    void logExtractVol(String source, long dataVol) {
//        if (dataVol > 0L)
//            MetricLogging.measure(
//                    "B", dataVol, "Donkey", "Extract", "Merge", source, out.getLastFlushedTime().toString());
    }

    static String wrap(SqlServerColumnInfo columnInfo) {
        return columnInfo.isSpatialType()
                ? SqlStatementUtils.SQL_SERVER.quote(columnInfo.columnName)
                        + ".STAsBinary() AS "
                        + SqlStatementUtils.SQL_SERVER.quote(columnInfo.columnName)
                : SqlStatementUtils.SQL_SERVER.quote(columnInfo.columnName);
    }

    private static JsonNode coerceSpatial(byte[] bytes) {
        if (bytes == null) return null;
        else if (bytes.length == 0) return JSON.createObjectNode();
//        else return JSON.valueToTree(SpatialUtil.geoFromBytes(bytes));
        else return null;
    }
}
