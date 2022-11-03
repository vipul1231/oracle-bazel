package com.example.core;


import com.example.logger.ExampleLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.example.flag.FlagName;
import com.example.utils.ExampleClock;
import com.google.common.base.Joiner;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/** Standard configuration implemented by all integrations. */
public class StandardConfig {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    /** When a schema / table / column disappears from source, delete it after this amount of time */
    public static final Duration DELETE_AFTER = Duration.ofDays(7);

    /**
     * Show checkboxes next to tables so the user can exclude them
     *
     * @deprecated
     */
    @Deprecated private boolean supportsExclude = false;

    /** Exclude new tables by default */
    private boolean excludeByDefault = false;

    /** Exclude new columns by default */
    private boolean excludeColumnsByDefault = false;

    /** Enable new data notification via email */
    private boolean enableNewDataNotification = false;

    /** The last timestamp when the config was approved by customer */
    private Instant lastCustomerApprovedAt = null;

    private SortedMap<String, SchemaConfig> schemas = new TreeMap<>();

    @JsonIgnore
    public Set<TableName> includedTables() {
        SortedSet<TableName> includedTables = new TreeSet<>();

        this.schemas.forEach(
                (s, schemaConfig) -> {
                    if (schemaConfig.isActive()) {
                        schemaConfig
                                .getTables()
                                .forEach(
                                        (t, tableConfig) -> {
                                            if (!tableConfig.getExcludedByUser().orElse(false)
                                                    && !tableConfig.getExcludedBySystem().isPresent()
                                                    && !tableConfig.getDeletedFromSource().isPresent()) {

                                                includedTables.add(TableName.withSchema(s, t));
                                            }
                                        });
                    }
                });

        return includedTables;
    }

    @JsonIgnore
    public Map<TableName, String> excludedTables() {
        SortedMap<TableName, String> excludedTables = new TreeMap<>();

        this.schemas.forEach(
                (s, schemaConfig) -> {
                    if (schemaConfig.isActive()) {
                        schemaConfig
                                .getTables()
                                .forEach(
                                        (t, tableConfig) -> {
                                            if (tableConfig.getExcludedByUser().orElse(false)
                                                    || tableConfig.getExcludedBySystem().isPresent()
                                                    || tableConfig.getDeletedFromSource().isPresent()) {

                                                String exclusionReason;
                                                if (tableConfig.getExcludedBySystem().isPresent())
                                                    exclusionReason = tableConfig.getExcludedBySystem().get();
                                                else if (tableConfig.getDeletedFromSource().isPresent())
                                                    exclusionReason =
                                                            "Deleted from Source at "
                                                                    + tableConfig
                                                                    .getDeletedFromSource()
                                                                    .get()
                                                                    .toString();
                                                else exclusionReason = "Excluded by user";

                                                excludedTables.put(TableName.withSchema(s, t), exclusionReason);
                                            }
                                        });
                    }
                });

        return excludedTables;
    }

    @JsonIgnore
    public Map<TableName, String> excludedBySystemTables() {
        SortedMap<TableName, String> excludedTables = new TreeMap<>();

        this.schemas.forEach(
                (s, schemaConfig) -> {
                    if (schemaConfig.isActive()) {
                        schemaConfig
                                .getTables()
                                .forEach(
                                        (t, tableConfig) ->
                                                tableConfig
                                                        .getExcludedBySystem()
                                                        .ifPresent(
                                                                reason ->
                                                                        excludedTables.put(
                                                                                TableName.withSchema(s, t), reason)));
                    }
                });

        return excludedTables;
    }

    @JsonIgnore
    public Set<TableName> historyTables() {
        SortedSet<TableName> historyTables = new TreeSet<>();

        this.schemas.forEach(
                (s, schemaConfig) -> {
                    if (schemaConfig.isActive()) {
                        schemaConfig
                                .getTables()
                                .forEach(
                                        (t, tableConfig) -> {
                                            if (!tableConfig.getExcludedByUser().orElse(false)
                                                    && !tableConfig.getExcludedBySystem().isPresent()
                                                    && !tableConfig.getDeletedFromSource().isPresent()
                                                    && tableConfig.getSyncMode().equals(SyncMode.History)) {

                                                historyTables.add(TableName.withSchema(s, t));
                                            }
                                        });
                    }
                });

        return historyTables;
    }

    public Map<TableRef, SyncMode> syncModes() {
        Map<TableRef, SyncMode> syncModes = new HashMap<>();
        getSchemas()
                .forEach(
                        (schema, schemaConfig) ->
                                schemaConfig
                                        .getTables()
                                        .forEach(
                                                (table, tableConfig) ->
                                                        syncModes.put(
                                                                new TableRef(schema, table),
                                                                tableConfig.getSyncMode())));
        return syncModes;
    }

    @JsonIgnore
    public void updateFromSource(StandardConfig fromSource) {
        this.supportsExclude = fromSource.supportsExclude;

        fromSource.schemas.forEach(
                (schema, from) -> {
                    SchemaConfig into =
                            this.schemas.computeIfAbsent(
                                    schema,
                                    newSchema -> {
                                        SchemaConfig newTables = new SchemaConfig();

                                        // if the system is excluding this schema for some reason, we do not want to set
                                        // it to user excluded. The user didn't make a choice.
                                        if (!from.getExcludedBySystem().isPresent()) {
                                            newTables.setExcludedByUser(
                                                    Optional.of(
                                                            from.getNotRecommended().isPresent() || excludeByDefault));
                                        }
                                        newTables.setNotRecommended(from.getNotRecommended());

                                        return newTables;
                                    });

                    into.updateFromSource(from, excludeByDefault, excludeColumnsByDefault);
                });

        Set<String> deleted = new HashSet<>();

        for (String schema : this.schemas.keySet()) {
            if (!fromSource.schemas.containsKey(schema)) deleted.add(schema);
        }

        if (!deleted.isEmpty()) {
            LOG.warning("Schemas " + Joiner.on(", ").join(deleted) + " are no longer present in source");

            for (String schema : deleted) {
                SchemaConfig schemaConfig = this.schemas.get(schema);
                // TODO
//                Instant deletedFromSource = schemaConfig.getDeletedFromSource().orElseGet(ExampleClock.Instant::now);
                Instant deletedFromSource = ExampleClock.Instant;

                if (deletedFromSource.isBefore(ExampleClock.Instant.now().minus(DELETE_AFTER)))
                    this.schemas.remove(schema);
                else schemaConfig.setDeletedFromSource(Optional.of(deletedFromSource));
            }
        }
    }

    /** TODO check */
//    @JsonIgnore
//    public void updateFromUser(StandardConfigPatch fromUser) {
//        fromUser.excludeByDefault.ifPresent(exclude -> this.excludeByDefault = exclude);
//        fromUser.excludeColumnsByDefault.ifPresent(exclude -> this.excludeColumnsByDefault = exclude);
//        fromUser.enableNewDataNotification.ifPresent(enable -> this.enableNewDataNotification = enable);
//
//        fromUser.schemas.ifPresent(
//                schemas -> {
//                    schemas.forEach(
//                            (schema, from) -> {
//                                SchemaConfig into = this.schemas.get(schema);
//
//                                if (into != null) into.updateFromUser(from);
//                            });
//                });
//
//        fromUser.lastCustomerApprovedAt.ifPresent(this::setLastCustomerApprovedAt);
//    }

    @Override
    public String toString() {
        return "StandardConfig{"
                + "supportsExclude="
                + supportsExclude
                + ", excludeByDefault="
                + excludeByDefault
                + ", schemas="
                + schemas
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StandardConfig that = (StandardConfig) o;
        return supportsExclude == that.supportsExclude
                && excludeByDefault == that.excludeByDefault
                && excludeColumnsByDefault == that.excludeColumnsByDefault
                && enableNewDataNotification == that.enableNewDataNotification
                && Objects.equals(lastCustomerApprovedAt, that.lastCustomerApprovedAt)
                && Objects.equals(schemas, that.schemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                supportsExclude,
                excludeByDefault,
                excludeColumnsByDefault,
                enableNewDataNotification,
                lastCustomerApprovedAt,
                schemas);
    }

    /** For user facing logs This method should return what to be shown from standard config in User facing logs. */
    @JsonIgnore
    public LinkedHashMap<String, Object> userFacingDataForUpdatedConfig(StandardConfig oldConfig) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();

        Map<String, StandardConfig.SchemaDetails> added = new HashMap<>();
        Map<String, StandardConfig.SchemaDetails> removed = new HashMap<>();

        // Finding added things
        this.schemas.forEach(
                (schemaFromNewConfig, newSchemaConfig) -> {
                    // Find delta of tables to add when schema is present in old and new config
                    if (oldConfig.schemas.containsKey(schemaFromNewConfig)) {
                        newSchemaConfig
                                .getTables()
                                .forEach(
                                        (tableFromNewSchemaConfig, newTableConfig) -> {
                                            if (!oldConfig
                                                    .schemas
                                                    .get(schemaFromNewConfig)
                                                    .getTables()
                                                    .containsKey(tableFromNewSchemaConfig)) {
//                                                added.computeIfAbsent(schemaFromNewConfig, SchemaDetails::new)
//                                                        .addTable(tableFromNewSchemaConfig);
                                            }
                                        });
                    }
                    // Add all tables when schema is not present in new config
                    else {
//                        added.computeIfAbsent(schemaFromNewConfig, SchemaDetails::new)
//                                .addTables(newSchemaConfig.getTables().keySet());
                    }
                });

        // Finding removed things
        oldConfig.schemas.forEach(
                (schemaFromOldConfig, oldSchemaConfig) -> {
                    // Find delta of tables to remove when schema is present in old and new config
                    if (this.schemas.containsKey(schemaFromOldConfig)) {
                        oldSchemaConfig
                                .getTables()
                                .forEach(
                                        (tableFromOldSchemaConfig, oldTableConfig) -> {
                                            if (!this.schemas
                                                    .get(schemaFromOldConfig)
                                                    .getTables()
                                                    .containsKey(tableFromOldSchemaConfig)) {
//                                                removed.computeIfAbsent(schemaFromOldConfig, SchemaDetails::new)
//                                                        .addTable(tableFromOldSchemaConfig);
                                            }
                                        });
                    }
                    // Remove all tables when schema is not present in new config
                    else {
//                        removed.computeIfAbsent(schemaFromOldConfig, SchemaDetails::new)
//                                .addTables(oldSchemaConfig.getTables().keySet());
                    }
                });

        if (!added.isEmpty()) {
            values.put(SchemaChangeType.ADDITION.name(), added.values());
        }

        if (!removed.isEmpty()) {
            values.put(SchemaChangeType.REMOVAL.name(), removed.values());
        }

        return values;
    }

    public boolean isSupportsExclude() {
        return supportsExclude;
    }

    public void setSupportsExclude(boolean supportsExclude) {
        this.supportsExclude = supportsExclude;
    }

    public boolean isExcludeByDefault() {
        return excludeByDefault;
    }

    public void setExcludeByDefault(boolean excludeByDefault) {
        this.excludeByDefault = excludeByDefault;
    }

    public boolean isExcludeColumnsByDefault() {
        return excludeColumnsByDefault;
    }

    public void setExcludeColumnsByDefault(boolean exclude) {
        this.excludeColumnsByDefault = exclude;
    }

    public boolean isEnableNewDataNotification() {
        return enableNewDataNotification;
    }

    public void setEnableNewDataNotification(boolean enable) {
        this.enableNewDataNotification = enable;
    }

    public SortedMap<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public SchemaConfig getSchema(String key) {
        return schemas.get(key);
    }

    public void putSchema(String key, SchemaConfig schema) {
        schemas.put(key, schema);
    }

    public void setSchemas(SortedMap<String, SchemaConfig> schemas) {
        this.schemas = schemas;
    }

    public Optional<Instant> getLastCustomerApprovedAt() {
        return Optional.ofNullable(lastCustomerApprovedAt);
    }

    public void setLastCustomerApprovedAt(Instant timestamp) {
        this.lastCustomerApprovedAt = timestamp;
    }

    // Justification: this makes conditions more clear.
    @SuppressWarnings({"ConstantConditions", "java:S2589"})
    @JsonIgnore
    public SchemaChangeHandling getSchemaChangeHandling() {
        if (!excludeByDefault && !excludeColumnsByDefault) {
            return SchemaChangeHandling.ALLOW_ALL;
        }

        if (excludeByDefault && !excludeColumnsByDefault) {
            return SchemaChangeHandling.ALLOW_COLUMNS;
        }

        if (excludeByDefault && excludeColumnsByDefault) {
            return SchemaChangeHandling.BLOCK_ALL;
        }

        throw new IllegalStateException(
                "Unsupported combination of excludeByDefault and excludeColumnsByDefault fields");
    }

    public void setSchemaChangeHandling(SchemaChangeHandling mode) {
        switch (mode) {
            case ALLOW_ALL:
                excludeByDefault = false;
                excludeColumnsByDefault = false;
                break;
            case ALLOW_COLUMNS:
                excludeByDefault = true;
                excludeColumnsByDefault = false;
                break;
            case BLOCK_ALL:
                excludeByDefault = true;
                excludeColumnsByDefault = true;
                break;
            default:
                throw new IllegalArgumentException("Unsupported value of SchemaChangeHandling enum: '" + mode + "'");
        }
    }

    @JsonIgnore
    public boolean isBlockAllMode() {
        return FlagName.AllowedList.check() && getSchemaChangeHandling() == SchemaChangeHandling.BLOCK_ALL;
    }

    public void setTables(Set<TableRef> tables) {

    }

    // Used for creating user facing log details
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SchemaDetails {
        public String schema;
        public List<String> tables = new ArrayList<>();

        // For JSON parsing
        public SchemaDetails() {}

        public SchemaDetails(String schema) {
            this.schema = schema;
        }

        void addTable(String table) {
            this.tables.add(table);
        }

        void addTables(Set<String> tables) {
            this.tables.addAll(tables);
        }
    }

    // Used for creating user facing log details
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TableDetails {
        public String schema;
        public String table;
        public List<String> columns = new ArrayList<>();

        // For JSON parsing
        public TableDetails() {}

        public TableDetails(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        void addColumn(String column) {
            this.columns.add(column);
        }
    }

    public enum SchemaChangeType {
        // Tables included automatically when user selects include new schema and tables automatically
        ADDITION,
        REMOVAL,

        // Schemas enabled and disabled through UI
        DISABLED,
        ENABLED,

        // Tables enabled and disabled through UI
        DISABLED_TABLES,
        ENABLED_TABLES,

        // Columns enabled and disabled through UI
        DISABLED_COLUMNS,
        ENABLED_COLUMNS,
        ;
    }

    public enum SchemaChangeHandling {
        ALLOW_ALL("Allow all"),
        ALLOW_COLUMNS("Allow columns"),
        BLOCK_ALL("Block all");

        private final String displayName;

        SchemaChangeHandling(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    @JsonIgnore
    public static StandardConfig createReadOnly(String schemaName, Collection<String> tableNames) {
        StandardConfig standardConfig = new StandardConfig();
        standardConfig.supportsExclude = false;
        SchemaConfig schemaConfig = new SchemaConfig();

        tableNames.forEach(
                name -> {
                    schemaConfig.putTable(name, TableConfig.withSupportsExclude(false));
                });

        standardConfig.schemas.put(schemaName, schemaConfig);

        return standardConfig;
    }

    @JsonIgnore
    public static StandardConfig createModifiable(String schemaName, Collection<String> tableNames) {
        StandardConfig standardConfig = new StandardConfig();
        standardConfig.supportsExclude = false;
        SchemaConfig schemaConfig = new SchemaConfig();

        tableNames.forEach(
                name -> {
                    schemaConfig.putTable(name, TableConfig.withSupportsExclude(true));
                });

        standardConfig.schemas.put(schemaName, schemaConfig);

        return standardConfig;
    }

    /** updates the config so that only one schema (first by name) is included */
    public void enforceSingleSchema() {
        boolean activeSchemaFound = false;

        for (SchemaConfig schema : schemas.values()) {
            if (!activeSchemaFound) {
                activeSchemaFound = schema.isActive();
            } else if (schema.isActive()) {
                schema.setExcludedByUser(Optional.of(true));
            }
        }
    }
}
