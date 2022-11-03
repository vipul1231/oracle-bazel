package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.snowflakecritic.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseBenchmarkRunner extends BaseScriptRunner {
    protected SnowflakeConnectorServiceConfiguration configuration;

    public static class BenchmarkResults {
        public SnowflakeTableInfo tableInfo;
        public long rowCount;
        public double updatePercent;
        public long testDurationMillis;
    }

    public BaseBenchmarkRunner(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
        configuration =
                new SnowflakeConnectorServiceConfiguration()
                        .setCredentials(source.originalCredentials)
                        .setStandardConfig(new StandardConfig())
                        .setSnowflakeSource(source);
    }

    protected abstract void runBenchmarkTest(SnowflakeTableInfo tableInfo) throws Exception;

    @Override
    public final void run() {
        try {
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

            runBenchmarkTest(tables.get(tableRefs.get(choice - 1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getUserChoice(int numberOfChoices) throws IOException {
        while (true) {
            System.out.print("Select a number from 0 to " + numberOfChoices + ": ");
            byte[] input = new byte[4];
            System.in.read(input);
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

    protected void waitBeforeNextTest(long seed) {
        try {
            Thread.sleep((long) Math.floor(seed * 35.0 / 1e8) * 60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

