package com.example.oracle;

import com.example.core.*;
import com.example.core.annotations.DataType;
import com.example.core.warning.Warning;
import com.example.core2.Output;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.oracle.exceptions.TypePromotionNeededException;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/28/2021<br/>
 * Time: 6:42 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

public class OracleOutputHelper {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    final Output<OracleState> output;
    private final String source;
    final OracleMetricsHelper metricsHelper;

    final Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
    private final Map<TableName, TableDefinition> tableDefs =
            new HashMap<>(); // Delete when cleaning up OracleFollowNewSubmitRecordCodePath
    private final Collection<TableRef> alreadySetupTables = new HashSet<>();
    private final Map<TableRef, Boolean> tableHasPKcache = new HashMap<>();

    private final Map<TableRef, SyncMode> syncModes;
//    private final Set<TableRef> selectedTables;
//    private final Set<TableRef> historyTables;
    private Map<TableRef, List<Column>> outputSchema;

    private long extractVol = 0L;

    private static final Column[] emptyColumnArray = new Column[0];

    OracleOutputHelper(
            Output<OracleState> output,
            String source,
            StandardConfig standardConfig,
            OracleMetricsHelper metricsHelper) {
        this.output = output;
        this.source = source;
        this.metricsHelper = metricsHelper;
        this.syncModes = standardConfig.syncModes();
//        this.historyTables =
//                standardConfig
//                        .historyTables()
//                        .stream()
//                        .map(t -> new TableRef(t.schema.get(), t.table))
//                        .collect(Collectors.toSet());
//        this.selectedTables =
//                standardConfig
//                        .includedTables()
//                        .stream()
//                        .map(t -> new TableRef(t.schema.get(), t.table))
//                        .collect(Collectors.toSet());
    }

    /** @return Tables that are selected by the user, and not excluded by the system */
    Set<TableRef> selectedTables() {
//        return selectedTables;
        return null;
    }

    /** @return Tables in history mode */
    Set<TableRef> historyTables() {
//        return historyTables;
        return null;
    }

    /** @return Map of TableRef to SyncMode */
    Map<TableRef, SyncMode> syncModes() {
        return syncModes;
    }

    void saveTableDefs(Map<TableRef, List<OracleColumn>> selectedTables, Set<TableRef> tablesToImport) {
//        this.outputSchema = Maps.transformEntries(selectedTables, OracleIncrementalUpdater::asOutputColumns);
        selectedTables.forEach(
                (tableRef, oracleColumns) -> {
                    SyncMode syncMode = syncModes().get(tableRef);
                    List<Column> tableSetupColumns = new ArrayList<>(asOutputColumns(tableRef, oracleColumns));
//                    if (syncMode == Legacy) {
//                        tableSetupColumns = addLegacyColumnsOld(tableSetupColumns);
//                    }
//                    if (!tableHasPK(tableRef, selectedTables)) {
//                        tableSetupColumns.add(Column.String(Names.example_ID_COLUMN).primaryKey(true).build());
//                    }
//                    recordSetupTable(
//                            tableRef, tableSetupColumns, tablesToImport.contains(tableRef), syncMode == History);
                });
    }

    private Boolean tableHasPK(TableRef tableRef, Map<TableRef, List<OracleColumn>> selected) {
        return tableHasPKcache.computeIfAbsent(tableRef, t -> selected.get(t).stream().anyMatch(c -> c.primaryKey));
    }

    private List<Column> asOutputColumns(TableRef table, List<OracleColumn> columns) {
        return columns.stream().map(c -> c.asexampleColumn(table.schema)).collect(Collectors.toList());
    }

    void submitRecordOld(
            Map<String, Object> values,
            TableRef tableRef,
            List<Column> columns,
            Instant changeTime,
            ChangeType changeType) {
        TableName tableName = toTableNameWithPrefix(source, tableRef);
        SyncMode syncMode = syncModes().get(tableRef);
//        if (syncMode == Legacy) {
//            columns = addLegacyColumnsOld(columns);
//            addLegacyColumnValues(values, changeType);
//        }
        Record record = prepareRecord(values, tableName, columns);
        TableDefinition tableDef = tableDefinition(tableName, columns, source);
        switch (changeType) {
            case INSERT:
                recordUpsert(tableDef, record, changeTime, syncMode);
                break;
            case UPDATE:
                recordUpdate(tableDef, record, changeTime, syncMode);
                break;
            case DELETE:
                recordDelete(tableDef, record, changeTime, syncMode);
                break;
            default:
                // To make Sonalint happy. No chance to get here. ChangeType only has above 3 values.
        }
//        measureExtractVol(DbRowSize.mapValues(values));
    }

    void submitRecord(
            Map<String, Object> values,
            TableRef tableRef,
            final List<Column> columns,
            Instant changeTime,
            ChangeType changeType) {
        if (!FlagName.OracleFollowNewSubmitRecordCodePath.check()) {
            submitRecordOld(values, tableRef, columns, changeTime, changeType);
            return;
        }
        SyncMode syncMode = syncModes().get(tableRef);
//        if (syncMode == Legacy) {
            addLegacyColumns(columns);
            addLegacyColumnValues(values, changeType);
//        }

        TableDefinition tableDef = tableDefinition(tableRef, columns);

        switch (changeType) {
            case INSERT:
                recordUpsert(tableDef, values, changeTime, syncMode);
                break;
            case UPDATE:
                recordUpdate(tableDef, values, changeTime, syncMode);
                break;
            case DELETE:
                recordDelete(tableDef, values, changeTime, syncMode);
                break;
            default:
                // To make Sonalint happy. No chance to get here. ChangeType only has above 3 values.
        }
//        measureExtractVol(DbRowSize.mapValues(values));
    }

    private void measureExtractVol(long bytes) {
//        metricsHelper.incrementRowSizeCounters(bytes);
        extractVol += bytes;
    }

    void warn(Warning warning) {
        output.warn(warning);
    }

    void recordUpsert(TableDefinition tableDef, Record record, Instant changeTime, SyncMode syncMode) {
//        if (syncMode == History) {
//            output.upsert(tableDef, record, changeTime);
//        } else {
//            output.upsert(tableDef, record);
//        }
    }

    void recordUpdate(TableDefinition tableDef, Record record, Instant changeTime, SyncMode syncMode) {
//        if (syncMode == History) {
//            output.update(tableDef, record, changeTime);
//        } else {
//            output.update(tableDef, record);
//        }
    }

    void recordDelete(TableDefinition tableDef, Record record, Instant changeTime, SyncMode syncMode) {
//        switch (syncMode) {
//            case History:
//                output.delete(tableDef, record, changeTime);
//                break;
//            case Live:
//                output.delete(tableDef, record);
//                break;
//            case Legacy:
//                output.update(tableDef, record);
//                break;
//            default:
//                throw new UnsupportedOperationException("No sync mode set for table " + tableDef.tableRef.toString());
//        }
    }

    void recordUpsert(TableDefinition tableDef, Map<String, Object> values, Instant changeTime, SyncMode syncMode) {
//        if (syncMode == History) {
//            output.upsert(tableDef, values, changeTime);
//        } else {
//            output.upsert(tableDef, values);
//        }
    }

    void recordUpdate(TableDefinition tableDef, Map<String, Object> values, Instant changeTime, SyncMode syncMode) {
//        if (syncMode == History) {
//            output.update(tableDef, values, changeTime);
//        } else {
//            output.update(tableDef, values);
//        }
    }

    void recordDelete(TableDefinition tableDef, Map<String, Object> values, Instant changeTime, SyncMode syncMode) {
//        switch (syncMode) {
//            case History:
//                output.delete(tableDef, values, changeTime);
//                break;
//            case Live:
//                output.delete(tableDef, values);
//                break;
//            case Legacy:
//                output.update(tableDef, values);
//                break;
//            default:
//                throw new UnsupportedOperationException("No sync mode set for table " + tableDef.tableRef.toString());
//        }
    }

    Record prepareRecord(Map<String, Object> values, TableName tableName, List<Column> columns) {
        Column[] columnsArray = columns.toArray(emptyColumnArray);
        Object[] valuesArray = new Object[columnsArray.length];
        for (int index = 0; index < columnsArray.length; index++) {
//            valuesArray[index] = values.getOrDefault(columnsArray[index].name, null);
        }
//        Record.Type type = new Record.Type(tableName, columnsArray);
//        return new Record(type, valuesArray, columns);
        return null;
    }

    static TableName toTableNameWithPrefix(String schemaPrefix, TableRef table) {
//        return TableName.withSchema(schemaPrefix + "_" + table.schema, table.name);
        return null;
    }

    static TableRef toTableRefWithPrefix(String schemaPrefix, TableRef table) {
        return new TableRef(schemaPrefix + "_" + table.schema, table.name);
    }

    void checkpoint(OracleState state) {
        output.checkpoint(state);
    }

    // >>>> TO be deleted from here to END below when cleaning up OracleFollowNewSubmitRecordCodePath FF
    public static TableName getTableNameWithSchema(TableName incoming, String source) {
//        return incoming.schema.isPresent() ? incoming : TableName.withSchema(source, incoming.table);
        return null;
    }

    public TableDefinition tableDefinition(TableName tableName, List<Column> columns, String source) {
        List<TableName> historyTableNames =
                historyTables().stream().map(t -> toTableNameWithPrefix(source, t)).collect(Collectors.toList());
        boolean isHistoryMode = historyTableNames.contains(tableName);
        return tableDefs.getOrDefault(tableName, tableDefinition(tableName, columns, source, isHistoryMode));
    }

    TableDefinition tableDefinition(TableName tableName, List<Column> columns, String source, boolean isHistoryMode) {
        TableName tableNameWithSchema = getTableNameWithSchema(tableName, source);

//        return tableDefs.computeIfAbsent(
//                tableNameWithSchema,
//                newTable -> {
//                    TableDefinition.Builder tableDefBuilder =
//                            TableDefinition.prepareTableDefinition(TableRef.fromTableName(newTable), columns)
//                                    .allowNullPrimaryKeys()
//                                    .withMutableColumnTypes();
//                    if (isHistoryMode) tableDefBuilder.withHistoryModeWrite();
//                    return tableDefBuilder.build();
//                });

        return null;
    }
    // <<<< END delete when cleaning up OracleFollowNewSubmitRecordCodePath FF.

    public TableDefinition tableDefinition(TableRef table, List<Column> columns) {
//        return tableDefinitions.computeIfAbsent(
//                table,
//                t -> {
//                    boolean isHistoryMode = historyTables.contains(table);
//                    TableDefinition.Builder tableDefBuilder =
//                            TableDefinition.prepareTableDefinition(toTableRefWithPrefix(source, table), columns)
//                                    .allowNullPrimaryKeys()
//                                    .withMutableColumnTypes();
//                    if (isHistoryMode) tableDefBuilder.withHistoryModeWrite();
//                    return tableDefBuilder.build();
//                });
        return null;
    }

    public TableDefinition tableDefinition(TableRef table, List<Column> columns, boolean isHistoryMode) {
//        return tableDefinitions.computeIfAbsent(
//                table,
//                t -> {
//                    TableDefinition.Builder tableDefBuilder =
//                            TableDefinition.prepareTableDefinition(toTableRefWithPrefix(source, table), columns)
//                                    .allowNullPrimaryKeys()
//                                    .withMutableColumnTypes();
//                    if (isHistoryMode) tableDefBuilder.withHistoryModeWrite();
//                    return tableDefBuilder.build();
//                });
        return null;
    }

    void recordSetupTable(TableRef tableRef, List<Column> columns, boolean isImport, boolean isHistoryMode) {
        if (!FlagName.OracleFollowNewSubmitRecordCodePath.check()) {
            recordSetupTableOld(tableRef, columns, isImport, isHistoryMode);
            return;
        }
        if (alreadySetupTables.contains(tableRef)) {
            return;
        }
        TableDefinition tableDefinition = tableDefinition(tableRef, columns, isHistoryMode);
        if (isImport) {
//            output.assertTableSchema(tableDefinition);
        }

        alreadySetupTables.add(tableRef);
    }

    // Delete this method when cleaning up OracleFollowNewSubmitRecordCodePath
    void recordSetupTableOld(TableRef tableRef, List<Column> columns, boolean isImport, boolean isHistoryMode) {
        if (alreadySetupTables.contains(tableRef)) {
            return;
        }
        TableName tableName = toTableNameWithPrefix(source, tableRef);
        TableDefinition tableDefinition = tableDefinition(tableName, columns, source, isHistoryMode);
        if (isImport) {
//            output.assertTableSchema(tableDefinition);
        }

        alreadySetupTables.add(tableRef);
    }

    void addLegacyColumnValues(Map<String, Object> row, ChangeType changeType) {
//        row.put(Names.example_DELETED_COLUMN, changeType == DELETE);
//        row.put(Names.example_SYNCED_COLUMN, exampleClock.Instant.now());
    }

    List<Column> addLegacyColumnsOld(List<Column> columns) {
        List<com.example.core.Column> columnsWithLegacy = new ArrayList<>(columns);
//        columnsWithLegacy.add(com.example.core.Column.Boolean(Names.example_DELETED_COLUMN).build());
//        columnsWithLegacy.add(com.example.core.Column.Instant(Names.example_SYNCED_COLUMN).build());
        return columnsWithLegacy;
    }

    Column[] legacyColumns = {
//            com.example.core.Column.Boolean(Names.example_DELETED_COLUMN).build(),
//            com.example.core.Column.Instant(Names.example_SYNCED_COLUMN).build()
    };

    List<Column> addLegacyColumns(List<Column> columns) {
        List<com.example.core.Column> columnsWithLegacy = columns;
        for (Column c : legacyColumns) columnsWithLegacy.add(c);
        return columnsWithLegacy;
    }

    void preImportDelete(TableRef tableRef, Instant syncBeginTime) {
        if (!FlagName.OracleFollowNewSubmitRecordCodePath.check()) {
            preImportDeleteOld(tableRef, syncBeginTime);
            return;
        }
        TableDefinition tableDef = tableDefinition(tableRef, getColumns(tableRef));
//        if (syncModes().get(tableRef) == Legacy) {
//            output.softDelete(tableDef, syncBeginTime);
//        } else {
//            output.hardDelete(tableDef, syncBeginTime);
//        }
    }

    // Delete this method when cleaning up OracleFollowNewSubmitRecordCodePath
    void preImportDeleteOld(TableRef tableRef, Instant syncBeginTime) {
        TableName tableName = toTableNameWithPrefix(source, tableRef);
        TableDefinition tableDef = tableDefinition(tableName, getColumns(tableRef), source);
//        if (syncModes().get(tableRef) == Legacy) {
//            output.softDelete(tableDef, syncBeginTime);
//        } else {
//            output.hardDelete(tableDef, syncBeginTime);
//        }
    }

    List<Column> getColumns(TableRef tableRef) {
        Objects.requireNonNull(outputSchema, "outputSchema is null. Did you forget to call saveTableDefs()?");
        return new ArrayList<>(outputSchema.get(tableRef));
    }

    Object coerce(TableRef tableRef, ResultSet rows, OracleColumn c) {
        try {
            Object value = coerceValue(tableRef, rows, c);

            if (rows.wasNull()) return null;
            else return value;
        } catch (SQLException e) {
            throw new RuntimeException("Error coercing row value", e);
        }
    }

    Object coerceValue(TableRef tableRef, ResultSet rows, OracleColumn c) throws SQLException {
        switch (c.jdbcExtractionType) {
            case String:
                return rows.getString(c.name);

            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
            case BigDecimal:
                // BigDecimal is more exact than Double, so even if the column type is Double, we want to use
                // BigDecimal to hold the value
                BigDecimal value = rows.getBigDecimal(c.name);
                if (value == null) {
                    return null;
                }
                if (FlagName.OracleExplicitTypeForNumbersAndDates.check()) {
                    return minimizeTypeWithPromotion(tableRef, c, value);
                } else {
                    return minimizeType(value);
                }
            case Binary:
                return rows.getBytes(c.name);

            case Instant:
                byte[] instantBytes = rows.getBytes(c.name);
                if (instantBytes == null) return null;

                return c.oracleType.instantFromBytes(instantBytes, c);

            case LocalDateTime:
                byte[] localDateTimeBytes = rows.getBytes(c.name);
                if (localDateTimeBytes == null) return null;
                return c.oracleType.localDateTimeFromBytes(localDateTimeBytes, c);

            case LocalDate:
                byte[] localDateBytes = rows.getBytes(c.name);
                if (localDateBytes == null) return null;
                try {
                    return c.oracleType.localDateFromBytes(localDateBytes, c);
                } catch (TypePromotionNeededException e) {
//                    promoteColumn(tableRef, c, DataType.LocalDateTime);
                    return OracleTimestamp.localDateTimeFromBytes(localDateBytes, c);
                }

            case Unknown:
                if (c.oracleType.isDate()) {
                    byte[] dateBytes = rows.getBytes(c.name);
                    if (dateBytes == null) return null;

//                    return OracleDate.anyFromBytes(dateBytes, c);
                } else {
                    return rows.getString(c.name);
                }

            default:
                throw new RuntimeException("Unrecognized type " + c.jdbcExtractionType);
        }
    }

    Object minimizeTypeWithPromotion(TableRef tableRef, OracleColumn column, BigDecimal value) {
        Object finalValue;
        switch (column.warehouseType) {
            case Short:
                finalValue =
                        (value.precision() > 4)
                                ? promoteNumberColumnAndValue(tableRef, column, value)
                                : value.shortValueExact();
                break;
            case Int:
                finalValue =
                        (value.precision() > 9)
                                ? promoteNumberColumnAndValue(tableRef, column, value)
                                : value.intValueExact();
                break;
            case Long:
                finalValue =
                        (value.precision() > 18)
                                ? promoteNumberColumnAndValue(tableRef, column, value)
                                : value.longValueExact();
                break;
            case Float:
            case Double:
            case BigDecimal:
            default:
                finalValue =
                        (value.precision() > OracleType.DEFAULT_PRECISION || value.scale() > OracleType.MAX_SCALE)
                                ? promoteNumberColumnAndValue(tableRef, column, value)
                                : value;
        }
        return finalValue;
    }

    /* This method takes BigDecimal values and tries to fit them into smaller type containers. This is so Core doesn't make all numeric columns into Float columns. */
    static Object minimizeType(BigDecimal value) {
        try {
            if (value.precision() >= OracleType.DEFAULT_PRECISION) {
                // for really big numbers, we need to send them to core as a String. Then core will type-promote the
                // column.
                // doing .stripTrailingZeros().toPlainString() "normalizes" the values (0.100 and 0.10 become 0.1) and
                // spits them out without scientific notation. Since the column type is going to be Unknown, this lets
                // core decide how to adjust the column to fit the value.
                return value.stripTrailingZeros().toPlainString();
            }

            // If we leave non-fractional (round) numbers as BigDecimal, core will make the column a float. We only want
            // to make the column a float if it actually contains a fraction value.
            if (value.scale() == 0) {
                // Round numbers
                if (value.precision() < 5) {
                    return value.shortValueExact();
                } else if (value.precision() < 10) {
                    return value.intValueExact();
                } else if (value.precision() < 19) {
                    return value.longValueExact();
                }
                // BigInteger is the next smalled type, but core turns them into BigDecimal anyway
            }
        } catch (ArithmeticException ex) {
            LOG.log(Level.INFO, "Failed to shrink BigDecimal to a smaller format", ex);
        }
        // doing .stripTrailingZeros().toPlainString() "normalizes" the values (0.100 and 0.10 become 0.1) and spits
        // them out without scientific notation. Since the column type is going to be Unknown, this lets core decide how
        // to adjust the column to fit the value.
        return new BigDecimal(value.stripTrailingZeros().toPlainString());
    }

    Object promoteNumberColumnAndValue(TableRef tableRef, OracleColumn column, BigDecimal value) {
//        DataType outputType = DataType.String;
//        if (value.precision() > OracleType.DEFAULT_PRECISION || value.scale() > OracleType.MAX_SCALE) {
//            outputType = DataType.String;
//        } else if (value.precision() > 18) {
//            outputType = DataType.BigDecimal;
//        } else if (value.precision() > 9) {
//            outputType = DataType.Long;
//        } else if (value.precision() > 4) {
//            outputType = DataType.Int;
//        }

//        promoteColumn(tableRef, column, outputType);
//
//        switch (outputType) {
//            case Int:
//                return value.intValueExact();
//            case Long:
//                return value.longValueExact();
//            case BigDecimal:
//                return value;
//            default:
//                return value.stripTrailingZeros().toPlainString();
//        }

        return null;
    }

    void promoteColumn(TableRef tableRef, OracleColumn column, DataType outputType) {
        LOG.info("Promoting " + tableRef.toString() + "." + column.name + " to " + outputType);
        TableDefinition existingTableDef =
                FeatureFlag.check("OracleFollowNewSubmitRecordCodePath")
                        ? tableDefinitions.get(tableRef)
                        : tableDefs.get(toTableNameWithPrefix(source, tableRef));
//        ColumnType updatedColumnType =
//                new ColumnType(outputType, OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty(), false);
//        existingTableDef.promoteType(column.name, updatedColumnType);
        column.updateWarehouseType(outputType);
    }

    long getExtractVol() {
        return extractVol;
    }

    public Object minimizeNumber(TableRef tableRef, OracleColumn sourceColumn, Object value) {
        BigDecimal bigDecimal = asBigDecimal(value);
        return minimizeTypeWithPromotion(tableRef, sourceColumn, bigDecimal);
    }

    static BigDecimal asBigDecimal(Object value) {

        if (value instanceof Number) {
            Number number = (Number) value;
            if (number instanceof BigDecimal) {
                return (BigDecimal) number;
            }

            if (number instanceof Long) {
                return new BigDecimal(number.longValue());
            }

            if (number instanceof Float || number instanceof Double) {
                return BigDecimal.valueOf(number.doubleValue());
            }

            return new BigDecimal(number.intValue());
        }

        return new BigDecimal(value.toString());
    }

    //@TODO need to set logic for historymode
    public boolean hasHistoryMode() {
       return Boolean.TRUE;
    }

    /**
     * @TODo: Need to write business logic
     * @param tableRef
     */
    public void saveLogminerJailTableDef(TableRef tableRef) {
    }

    /**
     * @TODO: Need to write business logic for same
     * @param table
     * @return
     */
    public TableRef tableRefWithSchemaPrefix(TableRef table) {
        return null;
    }

    /**
     * @TODO: Need to add business logic
     * @param table
     * @return
     */
    public TableDefinition existingTableDefinition(TableRef table) {
        return null;
    }

    /**
     * @TODO: Need to add business logic
     * @param table
     * @param updateSchemaOperation
     */
    public void updateSchema(TableRef table, UpdateSchemaOperation updateSchemaOperation) {
    }
}