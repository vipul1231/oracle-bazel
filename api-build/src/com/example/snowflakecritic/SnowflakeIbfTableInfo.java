package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.ibf.schema.IbfColumnInfo;
import com.example.ibf.schema.IbfTableInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SnowflakeIbfTableInfo {
    private final IbfTableInfo ibfTableInfo;
    private Map<String, IbfColumnInfo> modifiedColumns = new HashMap<>();
    private final SnowflakeTableInfo snowflakeTableInfo;

    public SnowflakeIbfTableInfo(SnowflakeTableInfo snowflakeTableInfo) {
        this.snowflakeTableInfo = snowflakeTableInfo;

        Map<String, IbfColumnInfo> columns =
                snowflakeTableInfo
                        .includedColumnInfo()
                        .stream()
                        .collect(Collectors.toMap(c -> c.columnName, SnowflakeIbfTableInfo::fromColumnInfo));

        this.ibfTableInfo = new IbfTableInfo(snowflakeTableInfo.sourceTable, columns);
    }

    public TableRef getTableRef() {
        return snowflakeTableInfo.sourceTable;
    }

    public IbfTableInfo getIbfTableInfo() {
        return ibfTableInfo;
    }

    public Map<String, IbfColumnInfo> getModifiedColumns() {
        return modifiedColumns;
    }

    public void setModifiedColumns(Map<String, IbfColumnInfo> modifiedColumns) {
        this.modifiedColumns = modifiedColumns;
    }

    public SnowflakeTableInfo getSnowflakeTableInfo() {
        return snowflakeTableInfo;
    }

    public static IbfTableInfo fromTableInfo(SnowflakeTableInfo snowflakeTableInfo) {
        Map<String, IbfColumnInfo> columns =
                snowflakeTableInfo
                        .includedColumnInfo()
                        .stream()
                        .collect(Collectors.toMap(c -> c.columnName, SnowflakeIbfTableInfo::fromColumnInfo));
        return new IbfTableInfo(snowflakeTableInfo.sourceTable, columns);
    }

    public static IbfColumnInfo fromColumnInfo(SnowflakeColumnInfo columnInfo) {
        return new IbfColumnInfo(columnInfo.columnName, columnInfo.destinationType);
    }
}
