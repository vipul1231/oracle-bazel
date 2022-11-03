package com.example.sql_server;

import com.example.core.*;
import com.example.db.DbTableInfo;

import java.util.Map;
import java.util.Optional;

public class SqlDbTableHelper {

//    @Override
    public TableRef defaultTable() {
        return customTableRef(Optional.empty(), Optional.empty());
    }

//    @Override
    public TableRef customTable(String customTableName) {
        return customTableRef(Optional.empty(), Optional.of(customTableName));
    }

//    @Override
    public TableRef destinationTableRef(TableRef tableRef, com.example.core2.ConnectionParameters params) {
        return DbTableInfo.initDestinationTableRef(tableRef, params.schema);
    }

    public TableRef tableWithCustomSchema(String customSchemaName) {
        return customTableRef(Optional.of(customSchemaName), Optional.empty());
    }

    public TableRef customTable(String customSchemaName, String customTableName) {
        return customTableRef(Optional.of(customSchemaName), Optional.of(customTableName));
    }


    //Defines the standard naming format for test tables used by our Dockerized databases and {@link RecordComparator}.
    //The table names cannot be random since {@link RecordComparator} compares actual table output from {@link
    //MockOutput2} with expected table output from hard-coded json files.

    //@param customTableName - Used when test methods require more than one test table.

    private TableRef customTableRef(Optional<String> customSchemaName, Optional<String> customTableName) {
        return new TableRef(customSchemaName.orElse("default_schema"), customTableName.orElse("default_table"));
    }

//    @Override
    public StandardConfig createStandardConfig(Map<TableRef, SyncMode> tablesToUpdate) {
        StandardConfig standardConfig = new StandardConfig();
        tablesToUpdate.forEach(
                (table, syncMode) -> {
                    standardConfig.getSchemas().putIfAbsent(table.schema, new SchemaConfig());
                    TableConfig tableConfig = new TableConfig();
                    tableConfig.setSyncMode(syncMode);
                    standardConfig.getSchema(table.schema).putTable(table.name, tableConfig);
                });
        return standardConfig;
    }

}
