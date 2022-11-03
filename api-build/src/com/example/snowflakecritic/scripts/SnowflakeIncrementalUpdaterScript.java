package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.ConnectionParameters;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeIbfTableUpdater;
import com.example.snowflakecritic.SnowflakeIncrementalUpdater;
import com.example.snowflakecritic.SnowflakeInformationSchemaDao;
import com.example.snowflakecritic.SnowflakeInformer;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeTableInfo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

public class SnowflakeIncrementalUpdaterScript extends BaseScriptRunner {
    private SnowflakeConnectorServiceConfiguration configuration;

    public SnowflakeIncrementalUpdaterScript(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
        configuration =
                new SnowflakeConnectorServiceConfiguration()
                        .setConnectionParameters(source.getConnectionParameters())
                        .setCredentials(source.originalCredentials)
                        .setStandardConfig(new StandardConfig())
                        .setSnowflakeSource(source)
                        .setConnectorState(new SnowflakeConnectorState());
    }

    @Override
    public void run() {
        SnowflakeIncrementalUpdater updater = new SnowflakeIncrementalUpdater(configuration);

        SnowflakeInformer informer = configuration.getSnowflakeInformer();

        SnowflakeInformationSchemaDao informationSchemaDao = informer.newInformationSchemaDao();
        Map<TableRef, SnowflakeTableInfo> tables = informationSchemaDao.fetchTableInfo();

        List<TableRef> tableRefs = new ArrayList<>(tables.keySet());
        tableRefs.sort(TableRef::compareTo);

        System.out.println(
                "\n============================================\nchoose a benchmark test to run or 0 to exit: ");
        AtomicInteger selection = new AtomicInteger(0);
        tableRefs.forEach(
                tableRef ->
                        System.out.println(
                                selection.addAndGet(1)
                                        + ": "
                                        + tableRef
                                        + " (row count: "
                                        + tables.get(tableRef).estimatedRowCount.orElse(0L)
                                        + ", bytes: "
                                        + tables.get(tableRef).estimatedDataBytes.orElse(0L)
                                        + ")"));
        int choice = getUserChoice(selection.get());

        if (choice == 0) {
            System.out.println("Cancelling test");
            return;
        }
        TableRef choosen = tableRefs.get(choice - 1);
        IbfCheckpointManager checkpointManager = ((SnowflakeIbfTableUpdater)updater.getSnowflakeTableUpdater(choosen)).checkpointManager();
        try {
            checkpointManager.reset();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        updater.incrementalUpdate(new HashSet<>(Arrays.asList(choosen)));
    }

    protected int getUserChoice(int numberOfChoices)  {
        while (true) {
            System.out.print("Select a number from 0 to " + numberOfChoices + ": ");
            byte[] input = new byte[4];
            try {
                System.in.read(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String inputString = new String(input).trim();
            try {
                int choice = Integer.parseInt(inputString);

                if (choice <= numberOfChoices) {
                    return choice;
                }

                System.err.println(inputString + " is not a valid choice.");
            } catch (NumberFormatException ex) {
                System.err.println(inputString + " is not a valid choice.");
            }
        }
    }
}