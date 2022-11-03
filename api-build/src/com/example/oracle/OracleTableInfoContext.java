package com.example.oracle;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.logger.ExampleLogger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Sort of like a DI context (i.e. like with Spring) only super light-weight. Makes instantiating complex objects much
 * easier and maintainable. Responsible for instantiating OracleTableInfo objects per selected table and providing
 * access to them.
 */
public class OracleTableInfoContext implements OracleTableInfoProvider {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private OracleTableMetricsProvider tableMetricsProvider;
    private StandardConfig standardConfig;
    private OracleState oracleState;
    private Map<TableRef, List<OracleColumn>> selectedTables;

    private Map<TableRef, Pair<OracleTableInfo, OracleTableState>> tableInfoMap = new HashMap<>();

    public OracleTableInfoContext(
            OracleTableMetricsProvider tableMetricsProvider,
            OracleState oracleState,
            StandardConfig standardConfig,
            Map<TableRef, List<OracleColumn>> selectedTables) {
        this.tableMetricsProvider = tableMetricsProvider;
        this.oracleState = Objects.requireNonNull(oracleState, "OracleState cannot be null");
        this.standardConfig = Objects.requireNonNull(standardConfig, "StandardConfig cannot be null");
        this.selectedTables = Objects.requireNonNull(selectedTables);

        for (TableRef tableRef : selectedTables.keySet()) {
            tableInfoMap.put(tableRef, newOracleTableInfo(tableRef));
        }
    }

    @Override
    public OracleTableInfo getOracleTableInfo(TableRef tableRef) {
        return get(tableRef).left;
    }

    @Override
    public OracleTableState getOracleTableState(TableRef tableRef) {
        return get(tableRef).right;
    }

    @Override
    public Collection<OracleTableInfo> getAllOracleTableInfo() {
        return tableInfoMap.values().stream().map(p -> p.left).collect(Collectors.toSet());
    }

    private Pair<OracleTableInfo, OracleTableState> get(TableRef tableRef) {
        if (!selectedTables.containsKey(tableRef)) {
            throw new IllegalArgumentException(tableRef.toString() + " is not a selected table");
        }

        return tableInfoMap.get(tableRef);
    }

    private Pair<OracleTableInfo, OracleTableState> newOracleTableInfo(TableRef tableRef) {
        return new Pair(
                new OracleTableInfo(
                        tableRef,
                        standardConfig,
                        toColumnInfo(tableRef, selectedTables.get(tableRef)),
                        tableMetricsProvider),
                new OracleTableState(oracleState, tableRef));
    }

    private List<OracleColumnInfo> toColumnInfo(TableRef tableRef, List<OracleColumn> columns) {
        return columns.stream()
                .sorted(Comparator.comparing(o -> o.name))
                .map(c -> new OracleColumnInfo(c, c.asexampleColumn(tableRef.schema)))
                .collect(Collectors.toList());
    }

    @Override
    public Set<TableRef> getSelectedTables() {
        return selectedTables.keySet();
    }
}
