package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeTimeTravelTableImporter;

import java.io.IOException;

public class ImportTable extends BaseScriptRunner {
    private SnowflakeTimeTravelTableImporter tableImporter;
    SnowflakeConnectorServiceConfiguration configuration;

    public ImportTable(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);

        configuration = new SnowflakeConnectorServiceConfiguration();
        try {
            configuration.setCredentials(fileHelper.loadCredentials());
            configuration.setStandardConfig(new StandardConfig());
            configuration.setConnectorState(new SnowflakeConnectorState());
            tableImporter = new SnowflakeTimeTravelTableImporter(configuration, new TableRef("PUBLIC", "CONNECTOR_TABLE1"));

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void setOutput(Output output) {
        configuration.setOutput(output);
    }

    @Override
    public void run() {
        try {
            tableImporter.startImport();
            tableImporter.importPage();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
