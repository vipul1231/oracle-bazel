package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableConfig;
import com.example.core.TableName;
import com.example.oracle.ColumnConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.example.snowflakecritic.scripts.UserInputHelper.println;

public class StandardConfigHelper {

    private final JsonFileHelper fileHelper;

    public StandardConfigHelper(JsonFileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    public void printStandardConfig(StandardConfig standardConfig) {

        for (TableName includedTable : standardConfig.includedTables()) {

            TableConfig tableConfig =
                    standardConfig.getSchemas().get(includedTable.schema.get()).getTable(includedTable.table);

            println(UserInputHelper.TextColor.GREEN, String.format("%s [syncMode: %s]", includedTable, tableConfig.getSyncMode()));

            tableConfig
                    .getColumns()
                    .ifPresent(
                            stringColumnConfigMap -> {
                                for (Map.Entry<String, ColumnConfig> columnEntry : stringColumnConfigMap.entrySet()) {
                                    ColumnConfig columnConfig = columnEntry.getValue();
                                    System.out.println(
                                            "      "
                                                    + columnEntry.getKey()
                                                    + (columnConfig.getIsPrimaryKey().isPresent()
                                                    ? (columnConfig.getIsPrimaryKey().get() ? " <PK>" : "")
                                                    : ""));
                                }
                            });
        }
    }

    public void optionallySaveStandardConfig(StandardConfig standardConfig) throws IOException {
        UserInputHelper.printHeader("Do you want to save this StandardConfig to your connector directory?");

        System.out.println("(If a previous version exists then a backup will be created.)");
        if (UserInputHelper.getYesOrNo(" Save file?")) {
            fileHelper.writeStandardConfig(standardConfig);
        }
    }

    public StandardConfig merge(StandardConfig fromInformer) throws IOException {
        Optional<StandardConfig> standardConfigOptional = fileHelper.loadStandardConfig();

        if (standardConfigOptional.isPresent()) {
            StandardConfig fromUser = standardConfigOptional.get();

            Map<TableName, String> excludedTables = fromUser.excludedTables();

            fromInformer
                    .includedTables()
                    .forEach(
                            tableName -> {
                                if (excludedTables.containsKey(tableName)) {
                                    fromInformer
                                            .getSchema(tableName.schema.get())
                                            .getTable(tableName.table)
                                            .setExcludedByUser(Optional.of(true));
                                }
                            });
        }

        return fromInformer;
    }
}