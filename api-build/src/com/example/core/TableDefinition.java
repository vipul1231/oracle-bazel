package com.example.core;

import com.example.core.annotations.DataType;
import com.example.oracle.Names;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Joiner;

import java.util.*;

/**
 * The definition of the table will be dynamically inferred based on the actual rows flowing through, but you need to
 * specify primaryKey,
 */
public class TableDefinition {
    private static long serialVersionUID = 3801124242820219131L;

    public TableRef tableRef;
    public Set<String> primaryKey;
    public Set<String> deleteKey;
    public LinkedHashMap<String, ColumnType> types;
    public Map<String, TableRef> foreignKeys;
    public Map<String, String> explodeWithPrefix;
    public Set<String> sortKey;
    public Set<String> distKey;
    public boolean inferJson;
    public boolean jacksonCompatible;
    public boolean allowNullPrimaryKeys;
    public boolean isHistoryModeWrite;
    public boolean allowMutableColumnTypes;
    public String deletedColumn;
    //public DateTimeFormatterType instantFormatterType;
    //public DateTimeFormatterType localDateFormatterType;
    //public DateTimeFormatterType localDateTimeFormatterType;
    public String pseudoHistoryColumn;
    public boolean hasWeakPrimaryKey;

    private static ObjectMapper mapper =
            new ObjectMapper()
                    .registerModule(new Jdk8Module());
                    //.setAnnotationIntrospector(new CsvAnnotationIntrospector());

    public TableDefinition() {
        
    }
    
    private TableDefinition(
            TableRef tableRef,
            Set<String> primaryKey,
            Set<String> deleteKey,
            LinkedHashMap<String, ColumnType> types,
            Map<String, TableRef> foreignKeys,
            Map<String, String> explodeWithPrefix,
            Set<String> sortKey,
            Set<String> distKey,
            boolean inferJson,
            boolean allowNullPrimaryKeys,
            boolean isHistoryModeWrite,
            boolean jacksonCompatible,
            boolean allowMutableColumnTypes,
            String deletedColumn,
            //DateTimeFormatterType instantFormatterType,
            //DateTimeFormatterType localDateFormatterType,
            //DateTimeFormatterType localDateTimeFormatterType,
            String pseudoHistoryColumn) {
        this.tableRef = tableRef;
        this.primaryKey = primaryKey;
        this.deleteKey = deleteKey;
        this.types = types;
        this.foreignKeys = foreignKeys;
        this.explodeWithPrefix = explodeWithPrefix;
        this.sortKey = sortKey;
        this.distKey = distKey;
        this.inferJson = inferJson;
        this.allowNullPrimaryKeys = allowNullPrimaryKeys;
        this.isHistoryModeWrite = isHistoryModeWrite;
        this.jacksonCompatible = jacksonCompatible;
        this.allowMutableColumnTypes = allowMutableColumnTypes;
        this.deletedColumn = deletedColumn;
        //this.instantFormatterType = instantFormatterType;
        //this.localDateFormatterType = localDateFormatterType;
        //this.localDateTimeFormatterType = localDateTimeFormatterType;
        this.pseudoHistoryColumn = pseudoHistoryColumn;
        this.hasWeakPrimaryKey = (primaryKey.size() != deleteKey.size());
    }

    /**
     * @deprecated If you need to promote a column type in your connector, use {@link
     *     Output#promoteColumnType(TableDefinition, String, ColumnType)} instead.
     */
    @Deprecated
    public void promoteType(String columnName, ColumnType incomingType) {
        //TableDefinitionModifier.promoteColumnType(this, columnName, incomingType);
    }

    public static TableDefinition copyOf(TableDefinition original) {
        return new TableDefinition(
                original.tableRef,
                new HashSet<>(original.primaryKey),
                new HashSet<>(original.deleteKey),
                new LinkedHashMap<>(original.types),
                new HashMap<>(original.foreignKeys),
                new HashMap<>(original.explodeWithPrefix),
                new HashSet<>(original.sortKey),
                new HashSet<>(original.distKey),
                original.inferJson,
                original.allowNullPrimaryKeys,
                original.isHistoryModeWrite,
                original.jacksonCompatible,
                original.allowMutableColumnTypes,
                original.deletedColumn,
                //original.instantFormatterType,
                //original.localDateFormatterType,
                //original.localDateTimeFormatterType,
                original.pseudoHistoryColumn);
    }

    public static TableDefinition.Builder prepareTableDefinition(TableRef table, List<Column> knownTypes) {
        String[] primaryKeys = knownTypes.stream().filter(c -> c.primaryKey).map(c -> c.name).toArray(String[]::new);
        String[] deleteKey = knownTypes.stream().filter(c -> c.deleteFrom).map(c -> c.name).toArray(String[]::new);

        TableDefinition.Builder tableBuilder = TableDefinition.create(table);

        if (primaryKeys.length > 0) tableBuilder = tableBuilder.primaryKey(primaryKeys);
        if (deleteKey.length > 0) tableBuilder = tableBuilder.deleteKey(deleteKey);

        for (int t = 0; t < knownTypes.size(); t++) {
            Column c = knownTypes.get(t);

            switch (c.type) {
                case Short:
                    tableBuilder = tableBuilder.asShort(c.name);
                    break;
                case Int:
                    tableBuilder = tableBuilder.asInt(c.name);
                    break;
                case Float:
                    tableBuilder = tableBuilder.asFloat(c.name);
                    break;
                case String:
                    tableBuilder = tableBuilder.asString(c.name, c.byteLength);
                    break;
                case BigDecimal:
                    if (!c.precision.isPresent() || !c.scale.isPresent()) tableBuilder = tableBuilder.asDouble(c.name);
                    else tableBuilder = tableBuilder.asDecimal(c.name, c.precision.getAsInt(), c.scale.getAsInt());
                    break;
                case Json:
                    tableBuilder = tableBuilder.asJson(c.name);
                    break;
                case Long:
                    tableBuilder = tableBuilder.asLong(c.name);
                    break;
                case Double:
                    tableBuilder = tableBuilder.asDouble(c.name);
                    break;
                case Instant:
                    tableBuilder = tableBuilder.asInstant(c.name);
                    break;
                case Boolean:
                    tableBuilder = tableBuilder.asBoolean(c.name);
                    break;
                case LocalDate:
                    tableBuilder = tableBuilder.asLocalDate(c.name);
                    break;
                case LocalDateTime:
                    tableBuilder = tableBuilder.asLocalDateTime(c.name);
                    break;
                case Binary:
                    tableBuilder = tableBuilder.asBinary(c.name);
                    break;
                // Not specified is equivalent to Unknown
                default:
                    break;
            }

            if (c.foreignKey!=null && c.foreignKey.isPresent()) {
                TableRef referencedTable = new TableRef(table.schema, c.foreignKey.get());
                tableBuilder = tableBuilder.foreignKey(c.name, referencedTable);
            }

            if (c.distKey) {
                tableBuilder = tableBuilder.distKey(c.name);
            }

            if (c.sortKey) {
                tableBuilder = tableBuilder.sortKey(c.name);
            }
        }

        // This is a shim to match the core1 behavior.
        // inferJson=true is NOT the intended default behavior for core2!
        // We will eventually migrate all connectors to talk to core2 directly,
        // and connectors that want inferJson=true will specify it themselves
        tableBuilder.inferJson();

        return tableBuilder;
    }

    public static TableDefinition.Builder create(Class<?> beanClass, String schema) {
        return create(beanClass, "tableNameNeedChange", schema);
        //return create(beanClass, ClassRecordType.getTableNameFromAnnotation(beanClass), schema);
    }

    public static TableDefinition.Builder create(Class<?> beanClass, String tableName, String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) throw new RuntimeException("Schema name is required");

        TableRef tableRef = new TableRef(defaultOutputSchema(schemaName), tableName);
        return create(tableRef, beanClass);
    }

    /**
     * This method can be used by connectors to create table definition from a bean class using a provided {@link
     * TableRef}.
     *
     * <p>{@link TableRef} provided will used as is in the TableDefinition without any modification. So connectors
     * should not pass {@link ConnectionParameters#schema} value directly if it contains both schema and optional table
     * name, e.g., google sheets connector stores schema.table in {@link ConnectionParameters#schema}. This needs to be
     * validated as well when connectors decide to migrate to this method.
     *
     * @param tableRef A {@link TableRef} instance to be used in {@link TableDefinition}.
     * @param beanClass Bean class for which defined types need to be extracted to initialise known types in table
     *     definition.
     * @return A {@link TableDefinition.Builder} instance with initialised primary keys, delete keys, foreign keys,
     *     known types etc.
     */
    public static TableDefinition.Builder create(TableRef tableRef, Class<?> beanClass) {
        //List<Column> knownTypes = new SchemaGenerator(mapper).schema(beanClass, Optional.of(Collections.emptyList()));
        List<Column> knownTypes = new ArrayList<>();
        return prepareTableDefinition(tableRef, knownTypes);
    }

    private static String defaultOutputSchema(String schema) {
        if (schema.matches(".*\\..*")) {
            String[] parts = schema.split("\\.");

            return parts[0];
        } else {
            return schema;
        }
    }

    public static TableDefinition.Builder create(TableRef table) {
        return new TableDefinition.Builder(table);
    }

    public static class Builder {
        private TableRef table;
        private Set<String> primaryKey = Collections.emptySet();
        private Set<String> deleteKey = Collections.emptySet();
        private LinkedHashMap<String, ColumnType> types = new LinkedHashMap<>();
        private Map<String, TableRef> foreignKeys = new HashMap<>();
        private Map<String, String> explodeWithPrefix = new HashMap<>();
        private Set<String> sortKey = new HashSet<>();
        private Set<String> distKey = new HashSet<>();
        private boolean inferJson = false;
        private boolean allowNullPrimaryKeys = false;
        private boolean isHistoryModeWrite = false;
        private boolean allowMutableColumnTypes = false;
        private String deletedColumn = Names.example_DELETED_COLUMN;
        //private DateTimeFormatterType instantFormatterType = DateTimeFormatterType.DEFAULT_INSTANT_FORMATTER;
        //private DateTimeFormatterType localDateFormatterType = DateTimeFormatterType.DEFAULT_DATE_FORMATTER;
        //private DateTimeFormatterType localDateTimeFormatterType = DateTimeFormatterType.DEFAULT_DATETIME_FORMATTER;
        private String pseudoHistoryColumn = null;

        private boolean jacksonCompatible = false;

        public Builder(TableRef table) {
            this.table = table;
        }

        public TableDefinition build() {
            if (this.primaryKey.isEmpty()) {
                throw new RuntimeException(
                        "At least one primary key should be defined in TableDefinition for table: " + table);
            }

            return buildTableDefinition();
        }

        /** This is needed for creating table definition when source does not provide primary key values */
        public TableDefinition buildForHashId() {
            return buildTableDefinition();
        }

        private TableDefinition buildTableDefinition() {
            validatePseudoHistoryColumnInfo();

            return new TableDefinition(
                    this.table,
                    this.primaryKey,
                    this.deleteKey,
                    this.types,
                    this.foreignKeys,
                    this.explodeWithPrefix,
                    this.sortKey,
                    this.distKey,
                    this.inferJson,
                    this.allowNullPrimaryKeys,
                    this.isHistoryModeWrite,
                    this.jacksonCompatible,
                    this.allowMutableColumnTypes,
                    this.deletedColumn,
                    //this.instantFormatterType,
                    //this.localDateFormatterType,
                    //this.localDateTimeFormatterType,
                    this.pseudoHistoryColumn);
        }

        private void validatePseudoHistoryColumnInfo() {
            if (pseudoHistoryColumn == null) return;

            if (primaryKey.contains(pseudoHistoryColumn))
                throw new IllegalArgumentException("Pseudo History Column should not be a primary key;");
            if (!isHistoryModeWrite)
                throw new IllegalArgumentException(
                        "Pseudo History information can be carried only when the table is set to History Mode;");
            if (!isDateLikeColumn(types.get(pseudoHistoryColumn).type))
                throw new IllegalArgumentException(
                        "Pseudo History Column should have one of the following types - LocalDate / LocalDateTime / Instant;");
        }

        private boolean isDateLikeColumn(DataType type) {
            return type == DataType.Instant || type == DataType.LocalDateTime || type == DataType.LocalDate;
        }

        /**
         * These columns are the primary key of the table. If multiple rows are sent to {@link
         * Output#upsert(TableDefinition, Object, String...)} with the same primary key, the last row will win. If
         * multiple rows are sent to {@link Output#update(TableDefinition, Object, String...)} with the same primary
         * key, they will be combined. If you do not specify primaryKey, {@link Output} will create a synthetic primary
         * key _example_id by hashing the entire row.
         */
        public Builder primaryKey(String... columns) {
            if (!primaryKey.isEmpty())
                throw new RuntimeException(
                        String.format("You already called primaryKey(%s)", Joiner.on(", ").join(columns)));

            primaryKey = new HashSet<>(Arrays.asList(columns));
            // This can later be overrided by setting deleteKey(...)
            deleteKey = primaryKey;

            return this;
        }

        /**
         * When you call {@link Output#checkpoint(Object)}, delete all existing records which exactly match on these
         * columns. If deleteKey is not specified, primaryKey will be used instead.
         */
        public Builder deleteKey(String... columns) {
            deleteKey = new HashSet<>(Arrays.asList(columns));

            if (!deleteKey.isEmpty() && primaryKey.isEmpty())
                throw new RuntimeException("You must set primaryKey(...) before setting deleteKey(...)");

            if (!primaryKey.containsAll(deleteKey))
                throw new RuntimeException(
                        String.format(
                                "deleteKey(%s) must be a prefix of primaryKey(%s)",
                                Joiner.on(", ").join(primaryKey), Joiner.on(", ").join(deleteKey)));

            return this;
        }

        /** The named column references the primary key of the named table */
        public Builder foreignKey(String column, TableRef table) {
            foreignKeys.put(column, table);

            return this;
        }

        public Builder sortKey(String column) {
            sortKey.add(column);

            return this;
        }

        public Builder distKey(String column) {
            distKey.add(column);

            return this;
        }

        /**
         * By default, any properties with complex types (List, Map) will be ignored. If you set inferJson(), then these
         * properties will be included as JSON. You can specify a *single* JSON column using {@link
         * TableDefinition.Builder#asJson(String)}
         */
        public Builder inferJson() {
            inferJson = true;

            return this;
        }

        public Builder withHistoryModeWrite() {
            isHistoryModeWrite = true;

            return this;
        }

        public Builder withMutableColumnTypes() {
            allowMutableColumnTypes = true;

            return this;
        }

//        public Builder inferLocalDate(DateTimeFormatterType formatterType) {
//            localDateFormatterType = formatterType;
//
//            return this;
//        }
//
//        public Builder inferInstant(DateTimeFormatterType formatterType) {
//            instantFormatterType = formatterType;
//
//            return this;
//        }
//
//        public Builder inferLocalDateTime(DateTimeFormatterType formatterType) {
//            localDateTimeFormatterType = formatterType;
//
//            return this;
//        }

        /** Replace null values in primary key columns with {@link DataType#defaultValue()} */
        public Builder allowNullPrimaryKeys() {
            allowNullPrimaryKeys = true;

            return this;
        }

        // Pipeline will automatically infer the types (boolean, long, double, String, Json)
        // If you want to specify a more precise type, you can do so here

        /**
         * Specifes a column is a Decimal
         *
         * <p>If the warehouse doesn't support Decimal, the column will be created as Double {@link Output} won't infer
         * a decimal type.
         */
        public Builder asDecimal(String column, int precision, int scale) {
            types.put(column, ColumnType.decimal(precision, scale, false));

            return this;
        }

        /**
         * Specifies a column is a short
         *
         * <p>If the warehouse doesn't support short, the column will be created as int or long
         */
        public Builder asShort(String column) {
            types.put(column, ColumnType.fromType(DataType.Short, false));

            return this;
        }

        /**
         * Specifies a column is an int
         *
         * <p>If the warehouse doesn't support int, the column will be created as long
         */
        public Builder asInt(String column) {
            types.put(column, ColumnType.fromType(DataType.Int, false));

            return this;
        }

        /**
         * Specifies a column is a float
         *
         * <p>If the warehouse doesn't support float, the column will be created as double
         */
        public Builder asFloat(String column) {
            types.put(column, ColumnType.fromType(DataType.Float, false));

            return this;
        }

        /**
         * Specifies a column is a string with a predefined max-length
         *
         * <p>{@link Output} will infer string lengths based on the actual lengths of the data, so you should only
         * specify a maximum length if the following are true: - The source specifies a maximum possible length for the
         * column - Otherwise the actual strings may be longer than you expect and get truncated - The actual string
         * lengths are usually at least 50% of the specified maximum length - Otherwise, it's a waste to allocate a
         * large capacity that doesn't get used
         */
        public Builder asString(String column, int maxLength) {
            assert maxLength > 0;

            types.put(column, ColumnType.string(OptionalInt.of(maxLength), false));

            return this;
        }

        public Builder asString(String column) {
            types.put(column, ColumnType.string(OptionalInt.empty(), false));

            return this;
        }

        /**
         * Specifies a column is JSON. {@link TableDefinition.Builder#inferJson()} is simpler and preferred; you should
         * only use asJson when you need to specify SOME columns as JSON while excluding other JSON columns.
         *
         * <p>This will convert complex types like Map, List, or ObjectNode to JSON data structures like {} and []. This
         * will NOT parse a JSON string; for example the string "{\"id\":1}" would get loaded into the warehouse as a
         * string with extra quotes '"{\"id\":1}"'
         */
        public Builder asJson(String column) {
            types.put(column, ColumnType.json(OptionalInt.empty(), false));

            return this;
        }

        public Builder asBinary(String column) {
            types.put(column, ColumnType.binary(OptionalInt.empty(), false));

            return this;
        }
        /**
         * Species the column is a Map that should be exploded into multiple columns. Each key in the map should be
         * converted into a column with the name [prefix]_[key name]
         */
        public Builder explode(String column, String prefix) {
            explodeWithPrefix.put(column, prefix);

            return this;
        }

        /** Do not use this in new code! Use ObjectMapper to replicate the behavior of RecordMapper. */
        @Deprecated
        public Builder jacksonCompatible() {
            jacksonCompatible = true;

            return this;
        }

        // TODO: The following private methods are specifically to bridge from Record#knownTypes to core2
        private Builder asLong(String column) {
            types.put(column, ColumnType.fromType(DataType.Long, false));

            return this;
        }

        private Builder asDouble(String column) {
            types.put(column, ColumnType.fromType(DataType.Double, false));

            return this;
        }

        public Builder asInstant(String column) {
            types.put(column, ColumnType.fromType(DataType.Instant, false));

            return this;
        }

        private Builder asBoolean(String column) {
            types.put(column, ColumnType.fromType(DataType.Boolean, false));

            return this;
        }

        private Builder asLocalDate(String column) {
            types.put(column, ColumnType.fromType(DataType.LocalDate, false));

            return this;
        }

        private Builder asLocalDateTime(String column) {
            types.put(column, ColumnType.fromType(DataType.LocalDateTime, false));

            return this;
        }

        private Builder asString(String column, OptionalInt maxLength) {
            types.put(column, ColumnType.string(maxLength, false));

            return this;
        }

        /**
         * If a connector has a source side deleted column to track deletes, then example will need to use this value
         * instead of {@link Names.example_DELETED_COLUMN}
         *
         * @param column column to be used as the deleted column
         */
        public Builder withDeletedColumn(String column) {
            deletedColumn = column;

            return this;
        }

        /**
         * If a connector has a source side column to track pseudo history, then example will need to use this value.
         * Pseudo History column information will be carried only when table definition is set to history mode.
         *
         * <p>The pseudo history column type should be one of the supported types - LocalDate, LocalDateTime, Instant
         *
         * @param column column to be used as the pseudo history column
         */
        public Builder withPseudoHistoryColumn(String column) {
            pseudoHistoryColumn = column;

            return this;
        }
    }

    @Override
    public String toString() {
        return (tableRef != null ? tableRef.toString() : "");
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableRef,
                primaryKey,
                deleteKey,
                types,
                foreignKeys,
                explodeWithPrefix,
                sortKey,
                distKey,
                inferJson,
                jacksonCompatible,
                allowNullPrimaryKeys,
                isHistoryModeWrite,
                allowMutableColumnTypes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableDefinition that = (TableDefinition) o;
        return Objects.equals(tableRef, that.tableRef)
                && Objects.equals(primaryKey, that.primaryKey)
                && Objects.equals(deleteKey, that.deleteKey)
                && Objects.equals(types, that.types)
                && Objects.equals(foreignKeys, that.foreignKeys)
                && Objects.equals(explodeWithPrefix, that.explodeWithPrefix)
                && Objects.equals(sortKey, that.sortKey)
                && Objects.equals(distKey, that.distKey)
                && inferJson == that.inferJson
                && jacksonCompatible == that.jacksonCompatible
                && allowNullPrimaryKeys == that.allowNullPrimaryKeys
                && isHistoryModeWrite == that.isHistoryModeWrite
                && allowMutableColumnTypes == that.allowMutableColumnTypes
                && Objects.equals(deletedColumn, that.deletedColumn)
                //&& instantFormatterType == that.instantFormatterType
                //&& localDateFormatterType == that.localDateFormatterType
                //&& localDateTimeFormatterType == that.localDateTimeFormatterType
                && Objects.equals(pseudoHistoryColumn, that.pseudoHistoryColumn);
    }
    
    public void setTableRef(TableRef sourceTable) {
        this.tableRef = sourceTable;
    }

    public TableRef getTableRef() {
        return tableRef;
    }
}