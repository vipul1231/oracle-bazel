package com.example.snowflakecritic;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class SnowflakeTableInfoBuilder {
    private final TableRef tableRef;
    private int columnCounter;
    private List<SnowflakeColumnInfo> columnInfoList = new ArrayList<>();
    private SyncMode syncMode = SyncMode.Legacy;
    private String schemaPrefix = "";

    public SnowflakeTableInfoBuilder(String schema, String tableName) {
        tableRef = new TableRef(schema, tableName);
    }

    public SnowflakeTableInfoBuilder setSyncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public SnowflakeTableInfoBuilder schemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
        return this;
    }

    public SnowflakeTableInfoBuilder pkCol(String name, SnowflakeType snowflakeColumnType) {
        return addColumn(name, snowflakeColumnType, true, false);
    }

    public SnowflakeTableInfoBuilder col(String name, SnowflakeType snowflakeColumnType) {
        return addColumn(name, snowflakeColumnType, false, false);
    }

    public SnowflakeTableInfoBuilder addColumn(
            String name, SnowflakeType snowflakeColumnType, boolean isPk, boolean excluded) {
        columnInfoList.add(
                new SnowflakeColumnInfo(
                        tableRef,
                        name,
                        ++columnCounter,
                        snowflakeColumnType,
                        snowflakeColumnType.destinationType(),
                        OptionalInt.empty(),
                        OptionalInt.empty(),
                        OptionalInt.empty(),
                        false,
                        isPk,
                        false,
                        Optional.empty(),
                        excluded, false));

        return this;
    }

    public SnowflakeTableInfo build() {
        return new SnowflakeTableInfo(tableRef, schemaPrefix, columnInfoList, columnCounter, syncMode);
    }
}
