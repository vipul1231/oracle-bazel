package com.example.oracle;


import com.example.core.Column;
import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Sort of like DbTableInfo but lighter weight. Maybe some day this would be updated to extend DbTableInfo.
 *
 * <p>consider separating the stateful (processing) part into a separate class.
 */
public class OracleTableInfo {
    private static final int MAXIMUM_PAGE_LIMIT = 15_000;

    /** The source table */
    private final TableRef tableRef;

    /** Ordered list */
    private final List<OracleColumnInfo> incomingColumns;

    private final Map<String, OracleColumnInfo> incomingColumnsIndexed = new HashMap<>();

    private final List<OracleColumnInfo> primaryKeys;
    private final OracleTableMetricsProvider tableMetricsProvider;
    private final StandardConfig standardConfig;
    private Optional<Long> pageLimit = Optional.empty();

    // For convenience
    private final List<Column> columns;
    private final List<OracleColumn> oracleColumns;

    public OracleTableInfo(
            TableRef tableRef,
            StandardConfig standardConfig,
            List<OracleColumnInfo> columns,
            OracleTableMetricsProvider tableMetricsProvider) {
        this.tableRef = tableRef;
        this.standardConfig = standardConfig;
        this.incomingColumns = columns;

        // Index the incomingColumns
        for (OracleColumnInfo columnInfo : columns) {
            incomingColumnsIndexed.put(columnInfo.getName(), columnInfo);
        }

        this.primaryKeys =
                incomingColumns
                        .stream()
                        .filter(c -> c.isPrimaryKey())
                        .sorted(Comparator.comparing(OracleColumnInfo::getName))
                        .collect(Collectors.toList());

        this.tableMetricsProvider = tableMetricsProvider;

        this.columns =
                incomingColumns.stream().map(columnInfo -> columnInfo.getTargetColumn()).collect(Collectors.toList());
        this.oracleColumns =
                incomingColumns.stream().map(columnInfo -> columnInfo.getSourceColumn()).collect(Collectors.toList());
    }

    public List<OracleColumnInfo> getIncomingColumns() {
        return incomingColumns;
    }

    public List<OracleColumnInfo> getPrimaryKeys() {
        return primaryKeys;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public OptionalLong getTableSize() {
        return tableMetricsProvider.getEstimatedTableSizeInBytes(tableRef);
    }

    public SyncMode getSyncMode() {
        return standardConfig.syncModes().get(getTableRef());
    }

    public OptionalLong getTableRowCount() {
        return tableMetricsProvider.getTableRowCount(tableRef);
    }

    public int getPageLimit() {
        return Integer.min(Math.toIntExact(pageLimit.orElse((long) MAXIMUM_PAGE_LIMIT)), MAXIMUM_PAGE_LIMIT);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<OracleColumn> getOracleColumns() {
        return oracleColumns;
    }

    public void adjustPageLimit() {
        throw new UnsupportedOperationException();
    }

    public boolean isExcluded() {
        return false;
        //return standardConfig.excludedTables().containsKey(getTableRef().getName());
    }

    public OracleColumnInfo getColumnInfo(String columnName) {
        return incomingColumnsIndexed.get(columnName);
    }
}
