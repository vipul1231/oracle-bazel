package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core.TableName;
import com.example.core.TableRef;
import com.example.db.DbInformer;
import com.example.db.DbServiceType;
import com.example.oracle.ColumnConfig;
import com.example.oracle.ColumnConfigInformation;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SnowflakeInformer extends DbInformer<SnowflakeSource, SnowflakeTableInfo> {
    public SnowflakeInformer(SnowflakeSource snowflakeSource, StandardConfig standardConfig) {
        super(snowflakeSource, standardConfig);
    }

    @Override
    protected TableRef extractTable(TableName tableName) {
        return null;
//        return TableRef.fromTableName(tableName);
    }

    @Override
    public Set<TableRef> includedTables() {
        return standardConfig
                .includedTables()
                .stream()
                .map(this::extractTable)
//                .filter(lazyAllTableInfo::containsKey)
                .collect(Collectors.toSet());
    }

    @Override
    protected Map<TableRef, SnowflakeTableInfo> latestTableInfo() {
        return newInformationSchemaDao().fetchTableInfo();
    }

    public SnowflakeInformationSchemaDao newInformationSchemaDao() {
        return new SnowflakeInformationSchemaDao(source, standardConfig, excludedColumns());
    }

    @Override
    public DbServiceType inferServiceType() {
        return null;
    }

    @Override
    public Optional<String> version() {
        return Optional.empty();
    }

    @Override
    public Map<String, ColumnConfigInformation> fetchColumnConfigInfo(TableRef tableWithSchema) {
        return null;
    }


    //@Override
    public Map<String, ColumnConfig> fetchColumnConfig(TableRef tableRef) {
        return newInformationSchemaDao()
                .getTableColumnInfo(tableRef)
                .stream()
                .collect(Collectors.toMap(columnInfo -> columnInfo.columnName, columnInfo -> from(columnInfo)));
    }

    public StandardConfig getStandardConfig() {
        return standardConfig;
    }

    private ColumnConfig from(SnowflakeColumnInfo column) {
        ColumnConfig columnConfig = new ColumnConfig();
        columnConfig.setIsPrimaryKey(Optional.of(column.isPrimaryKey));
        return columnConfig;
    }
}
