package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core2.Output;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeSourceCredentials;

public class RunSync extends BaseServiceScript {

    public RunSync(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    public void run() {
        try (Output<SnowflakeConnectorState> output = new FileBasedScriptRunnerOutput(fileHelper)) {
            SnowflakeConnectorState state = getConnectorState();

            System.out.println("==== Credentials ====");
            printCredentials();

            printState("==== State Before Sync ====", state);
            StandardConfig standardConfig = getStandardConfig(state);

            System.out.println("\n\n================================\nYou are about to sync the following tables:\n");
            StandardConfigHelper standardConfigHelper = new StandardConfigHelper(fileHelper);
            standardConfigHelper.printStandardConfig(standardConfig);

            if (!UserInputHelper.getYesOrNo("\nDo you want to proceed?")) {
                return;
            }

            // If this is a ibf sync, then configure ibf now
            if (source.originalCredentials.updateMethod == SnowflakeSourceCredentials.UpdateMethod.IBF) {
                System.out.println("\n==== Configuring Ibf ====");
                SnowflakeConnectorServiceConfiguration.override(
                        IbfPersistentStorage.class, config -> localTestIbfPersistentStorage(state));
            }

            service.update(source.originalCredentials, state, source.params, output, standardConfig, null);

            printState("==== State After Sync ====", state);

            printMetrics();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void printMetrics() throws Exception {
        System.out.println(fileHelper.toString());
    }
}
