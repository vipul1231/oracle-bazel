package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.snowflakecritic.*;
import com.example.core2.ConnectionParameters;


import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class SnowflakeTableImporterScript extends BaseScriptRunner {

    private ConnectionParameters params = new ConnectionParameters("test", "test", TimeZone.getDefault());
    private SnowflakeConnectorState state = new SnowflakeConnectorState();

    public SnowflakeTableImporterScript(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    public void run() {
        StandardConfigHelper standardConfigHelper = new StandardConfigHelper(fileHelper);

        try {
            SnowflakeConnectorService service = new SnowflakeConnectorService(SnowflakeServiceType.SNOWFLAKE);

            StandardConfig standardConfig =
                    standardConfigHelper.merge(service.standardConfig(source.originalCredentials, params, state));

            SnowflakeInformer informer = service.informer(source, standardConfig);
            Set<TableRef> includedTables = informer.includedTables();
            Set<TableRef> excludedTables = informer.excludedTables();

            System.out.println("");
            System.out.println("Update Method: " + source.originalCredentials.updateMethod);
            System.out.println("");

            if (!excludedTables.isEmpty()) {
                System.out.println("The following tables are excluded:");
                for (TableRef name : excludedTables) {
                    System.out.println("  - " + name);
                }
                System.out.println("");
            }

            System.out.println("The following tables are included:");
            for (TableRef name : includedTables) {
                System.out.println("  + " + name);
            }
            System.out.println("");

            standardConfigHelper.optionallySaveStandardConfig(standardConfig);

            if (UserInputHelper.getYesOrNo("\nDo you want to import the included tables?")) {
                Output<SnowflakeConnectorState> output = new ScriptRunnerOutput();

                // Simplified version of DbService.update logic
                try {
                    SnowflakeImporter importer = service.importer(source, state, informer, output);

                    List<TableRef> sortedTablesToImport =
                            includedTables
                                    .stream()
                                    .filter(t -> !importer.importFinished(t))
                                    .sorted(importer.tableSorter())
                                    .collect(Collectors.toList());

                    for (TableRef tableRef : sortedTablesToImport) {
                        importer.beforeImport(tableRef);
                        importer.importPage(tableRef);
                        importer.importFinished(tableRef);
                        output.checkpoint(state);
                    }
                } finally {
                    //output.close();

                    if (UserInputHelper.getYesOrNo("Do you want to save the connector state?")) {
                        fileHelper.writeConnectorState(state);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
