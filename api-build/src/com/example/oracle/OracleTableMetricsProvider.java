package com.example.oracle;

import com.example.core.TableRef;

import java.util.OptionalLong;

public interface OracleTableMetricsProvider {
    OptionalLong getEstimatedTableSizeInBytes(TableRef tableRef);

    OptionalLong getTableRowCount(TableRef tableRef);
}
