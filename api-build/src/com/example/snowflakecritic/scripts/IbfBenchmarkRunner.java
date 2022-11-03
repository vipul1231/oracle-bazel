package com.example.snowflakecritic.scripts;

import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.ibf.SnowflakeIbfAdapter;

import java.util.concurrent.Callable;

public class IbfBenchmarkRunner extends BaseBenchmarkRunner {
    public static final int NUMBER_OF_ITERATIONS = 3;
    public static final double[] UPDATE_SIZE_PERCENTS = {0.0005, 0.005, .05};

    private static final double ALPHA = 1.5;

    public static class IbfBenchmarkResults extends BaseBenchmarkRunner.BenchmarkResults {
        public int cellCount = 0;
    }

    public IbfBenchmarkRunner(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    protected void runBenchmarkTest(SnowflakeTableInfo tableInfo) throws Exception {
        System.out.println("Running test " + tableInfo.sourceTable);

        SnowflakeIbfAdapter ibfAdapter = new SnowflakeIbfAdapter(source.dataSource, tableInfo);

        long rowCount = tableInfo.estimatedRowCount.orElse(1000L);

        boolean firstRun = true;


        for (double updatePercent : UPDATE_SIZE_PERCENTS) {
            IbfBenchmarkResults results = new IbfBenchmarkResults();
            results.tableInfo = tableInfo;
            results.updatePercent = updatePercent;
            results.rowCount = rowCount;
            results.cellCount = Math.max(300, (int) (rowCount * updatePercent * ALPHA));

            if (!firstRun) {
                waitBeforeNextTest(rowCount);
            }
            firstRun = false;

            results.testDurationMillis =
                    measureExecutionTime(() -> ibfAdapter.getInvertibleBloomFilter(results.cellCount));

            System.out.println(fileHelper.toJson(results));
        }
    }

    private long measureExecutionTime(Callable<?> function) throws Exception {
        long start = System.currentTimeMillis();
        function.call();
        return System.currentTimeMillis() - start;
    }
}

