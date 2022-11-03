package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableConfig;
import com.example.core.TableName;
import com.example.snowflakecritic.*;
import com.example.core2.ConnectionParameters;


import java.io.IOException;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;

import static com.example.snowflakecritic.scripts.UserInputHelper.*;

public class GetStandardConfig extends BaseServiceScript {

    public GetStandardConfig(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    public void run() {
        try {
            SnowflakeConnectorState state = getConnectorState();

            StandardConfig standardConfig = getStandardConfig(state);

            getUserExclusions(standardConfig);

            newLine();

            doGetOptionalInfo(service, standardConfig);

            printHeader("StandardConfig: Included Tables");
            standardConfigHelper.printStandardConfig(standardConfig);

            standardConfigHelper.optionallySaveStandardConfig(standardConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void doGetOptionalInfo(SnowflakeConnectorService service, StandardConfig standardConfig) {
        // Nothing to do in this case: override in subclasses as needed. See GetSchema.
    }

    private void getUserExclusions(StandardConfig standardConfig) throws IOException {
        printHeader("Select the tables you would like to include:");

        SortedSet<TableName> allTables = new TreeSet<>();

        standardConfig
                .getSchemas()
                .forEach(
                        (s, schemaConfig) -> {
                            if (schemaConfig.isActive()) {
                                schemaConfig
                                        .getTables()
                                        .forEach(
                                                (t, tableConfig) -> {
                                                    if (!tableConfig.getDeletedFromSource().isPresent()) {
                                                        allTables.add(TableName.withSchema(s, t));
                                                    }
                                                });
                            }
                        });

        for (TableName tableName : allTables) {
            boolean excludeTable = !getYesOrNo(" Include table " + tableName + "?");

            TableConfig tableConfig = standardConfig.getSchema(tableName.schema.get()).getTable(tableName.table);

            tableConfig.setExcludedByUser(Optional.of(excludeTable));

            if (!excludeTable) {
                SyncMode syncMode =
                        (SyncMode)
                                getUserChoice(
                                        "   Sync mode for " + tableName + ": ", SyncMode.Legacy, SyncMode.History);
                tableConfig.setSyncMode(syncMode);
            }
        }
    }
}