package com.example.db;

import com.example.core.*;
import com.example.core.warning.Warning;
import com.example.lambda.Lazy;
import com.example.logger.ExampleLogger;
import com.example.oracle.ColumnConfig;
import com.example.oracle.ColumnConfigInformation;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;

public abstract class DbInformer<Source extends AutoCloseable, TableInfo extends DbTableInfo<?>> {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    protected final Source source;
    protected StandardConfig standardConfig;
    protected final List<Warning> warnings = new ArrayList<>();

    /**
     * Because {@link com.example.core.TableDefinition}'s cannot be updated mid-sync, this method should only be
     * called once on a given sync.
     */
    private final Lazy<Map<TableRef, TableInfo>> lazyAllTableInfo;
//    private Lazy<Map<TableRef, TableInfo>> lazyAllTableInfo;
    /**
     * TODO - check the Lazy implementation
     */
//    public Map<TableRef, TableInfo> lazyAllTableInfo;

    protected DbInformer(Source source, StandardConfig standardConfig) {
        this.source = source;
        this.standardConfig = standardConfig;
//        this.lazyAllTableInfo = latestTableInfo();
        this.lazyAllTableInfo = new Lazy<>(this::latestTableInfo);
    }

    /**
     * Converts {@link TableName} to the {@link TableRef} determined by the subclass. This will usually be {@link
     * TableRef} for relational DB's or {@link String} for non-relational DB's.
     */
    protected abstract TableRef extractTable(TableName tableName);

    /**
     * Fetches the latest {@link TableInfo} for all source {@link TableRef}'s. This method was only intended for use in
     * {@link DbInformer}. Unless necessary, preserve the protected access modifier in concrete implementations.
     */
    protected abstract Map<TableRef, TableInfo> latestTableInfo();

    /**
     * Returns the key set of the {@link DbInformer#lazyAllTableInfo} cache, which contains a {@link TableRef} for all
     * source tables.
     *
     * <p>The output of this method should be independent of {@link DbInformer#standardConfig} so it can be used to used
     * to initialize {@link com.example.core.Service#standardConfig}.
     */

    public final Set<TableRef> allTables() {
        return lazyAllTableInfo.get().keySet();
    }

    /**
     * Returns the subset of {@link TableRef}'s from the {@link DbInformer#lazyAllTableInfo} key set that are marked as
     * included in the {@link StandardConfig}.
     *
     * <p>The output of this method is dependent upon {@link DbInformer#standardConfig}
     */
    public Set<TableRef> includedTables() {
        return standardConfig
                .includedTables()
                .stream()
                .map(this::extractTable)
                .peek(this::warnIfTableNotInSource)
                .filter(lazyAllTableInfo.get()::containsKey)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the subset of {@link TableRef}'s from the {@link DbInformer#lazyAllTableInfo} key set that are not marked
     * as included in the {@link StandardConfig}.
     *
     * <p>The output of this method is dependent upon {@link DbInformer#standardConfig}
     */
    public Set<TableRef> excludedTables() {
        Set<TableRef> includedTables = includedTables();
        return Sets.difference(allTables(), includedTables);
    }

    /**
     * Returns a {@link TableInfo} retrieved from the {@link DbInformer#lazyAllTableInfo} map. The method assumes the
     * caller is passing in a {@link TableRef} key that originated from {@link DbInformer#lazyAllTableInfo}. As such, a
     * {@link NoSuchElementException} is thrown if {@link DbInformer#lazyAllTableInfo#get} returns null.
     */
    public TableInfo tableInfo(TableRef table) throws NoSuchElementException {
        if (!lazyAllTableInfo.get().containsKey(table)) {
            throw new NoSuchElementException("DbInformer#allTableInfo does not contain " + table.toString());
        }

        return lazyAllTableInfo.get().get(table);
    }

    /**
     * Update the latest table infos for tables during a sync
     *
     * @param latestTableInfos map containing the latest table infos for tables
     */
    public void updateTableInfo(Map<TableRef, TableInfo> latestTableInfos) {
//        Map<TableRef, TableInfo> tableRefTableInfoMap = lazyAllTableInfo.get();
        Map<TableRef, TableInfo> tableRefTableInfoMap = null;
        latestTableInfos
                .keySet()
                .stream()
                .filter(tableRef -> latestTableInfos.get(tableRef) != null)
                .forEach(tableRef -> tableRefTableInfoMap.put(tableRef, latestTableInfos.get(tableRef)));
    }

    /**
     * Takes a source database {@link TableRef} and returns an {@link Optional} estimate of the table's size in
     * gigabytes.
     */
    public Optional<Double> estimatedSizeInGigaBytes(TableRef table) throws NoSuchElementException {
        if (tableInfo(table) == null) {
            return Optional.empty();
        }

        return tableInfo(table).estimatedSizeInGigaBytes();
    }

    /**
     * Takes a source database {@link TableRef} and returns its corresponding destination warehouse {@link TableRef}.
     */
    public TableRef destinationTableRef(TableRef table) {
        return tableInfo(table).destinationTableRef;
    }

    private void warnIfTableNotInSource(TableRef table) {
//        if (!lazyAllTableInfo.get().containsKey(table)) {
//            LOG.warning("Table in standard config not detected in source: " + table);
//        }
    }

    protected Set<String> userExcludedSchemas() {
        SortedSet<String> schemaNames = new TreeSet<>();

//        standardConfig
//                .getSchemas()
//                .forEach(
//                        (s, schemaConfig) -> {
//                            if (schemaConfig.getExcludedByUser().orElse(false)
//                                    && !schemaConfig.getExcludedBySystem().isPresent()
//                                    && !schemaConfig.getDeletedFromSource().isPresent()) {
//
//                                schemaNames.add(s);
//                            }
//                        });
//        return schemaNames;
        return null;
    }

    public List<Warning> dashboardWarnings() {
        return warnings;
    }

    public abstract DbServiceType inferServiceType();

    public abstract Optional<String> version();

    public abstract Map<String, ColumnConfigInformation> fetchColumnConfigInfo(TableRef tableWithSchema);

    /**
     * Returns the set of excluded columns on the table, or an empty set if the table is not part of the standard config
     * object (ie, in the case of system schemas and system tables).
     *
     * @param table - the table for which we're fetching excluded columns
     * @return a Set<String> representing the names of all columns the user expects to have us sync
     */
    public Set<String> excludedColumns(TableRef table) {
//        SchemaConfig schemaConfig = standardConfig.getSchema(table.schema);
        SchemaConfig schemaConfig = null;
        if (schemaConfig == null) return ImmutableSet.of();

//        TableConfig tableConfig = schemaConfig.getTable(table.name);
        TableConfig tableConfig = null;
        if (tableConfig == null) return ImmutableSet.of();

//        Map<String, ColumnConfig> columns = tableConfig.getColumns().orElse(new HashMap<>());
        Map<String, ColumnConfig> columns = null;

        return columns.entrySet()
                .stream()
//                .filter(e -> e.getValue().excluded())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public Map<TableRef, Set<String>> excludedColumns() {
//        return standardConfig
//                .includedTables()
//                .stream()
//                .map(TableRef::fromTableName)
//                .collect(Collectors.toMap(Function.identity(), this::excludedColumns));
        return new HashMap<TableRef, Set<String>>();
    }

    public Map<TableRef, SyncMode> syncModes() {
        return standardConfig.syncModes();
    }
}
