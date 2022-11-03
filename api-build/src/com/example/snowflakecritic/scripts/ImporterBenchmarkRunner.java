package com.example.snowflakecritic.scripts;

import com.example.core.ColumnType;
import com.example.core.StandardConfig;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.warning.Warning;
import com.example.core2.OperationTag;
import com.example.core2.Output;
import com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.SnowflakeTimeTravelTableImporter;

import java.time.Instant;

import static com.example.snowflakecritic.scripts.SnowflakeConnectorScriptRunner.DEFAULT_ROW_COUNT;

public class ImporterBenchmarkRunner extends BaseBenchmarkRunner {
    private SnowflakeTimeTravelTableImporter tableImporter;
    private SnowflakeConnectorState state = new SnowflakeConnectorState();

    public ImporterBenchmarkRunner(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    protected void runBenchmarkTest(SnowflakeTableInfo tableInfo) throws Exception {

        Output<SnowflakeConnectorState> output =
                new Output<SnowflakeConnectorState>() {
//                    @Override
//                    public TableDefinition promoteColumnType(
//                            TableDefinition tableDefinition, String columnName, ColumnType newType) {
//                        return null;
//                    }
//
//                    @Override
//                    public TableDefinition updateSchema(TableDefinition table, UpdateSchemaOperation updateSchema) {
//                        return null;
//                    }

                    @Override
                    public void upsert(TableDefinition table, Object value, OperationTag... tags) {}

                    @Override
                    public void upsert(
                            TableDefinition table, Object value, Instant operationTimestamp, OperationTag... tags) {}

                    @Override
                    public void update(TableDefinition table, Object diff, OperationTag... tags) {}

                    @Override
                    public void update(
                            TableDefinition table, Object diff, Instant operationTimestamp, OperationTag... tags) {}

                    @Override
                    public void delete(TableDefinition table, Object deleteKeys) {}

                    @Override
                    public void delete(TableDefinition table, Object deleteKeys, Instant operationTimestamp) {}

                    @Override
                    public void hardDelete(
                            TableRef tableRef, String deletedColumn, Instant deleteBefore, boolean historyMode) {}

                    @Override
                    public void softDelete(
                            TableRef table, String setIsDeletedColumn, String whereTimeColumn, Instant syncBeginTime) {}

                    //@Override
                    public void assertTableSchema(TableDefinition tableDef) {}

                    @Override
                    public void checkpoint(SnowflakeConnectorState snowflakeConnectorState) {}

//                    @Override
//                    public void signal(SignalType signalType, TableRef... tableRefs) {}
//
//                    @Override
//                    public RenamingFilter2 renamingFilter() {
//                        return null;
//                    }

                    @Override
                    public void warn(Warning warning) {}

                    @Override
                    public void close() {}
                };

        SnowflakeConnectorServiceConfiguration configuration =
                new SnowflakeConnectorServiceConfiguration()
                        .setCredentials(source.originalCredentials)
                        .setStandardConfig(new StandardConfig())
                        .setSnowflakeSource(source)
                        .setConnectorState(new SnowflakeConnectorState())
                        .setOutput(output);

        SnowflakeTimeTravelTableImporter tableImporter =
                new SnowflakeTimeTravelTableImporter(configuration, tableInfo.sourceTable);
        long rowCount = tableInfo.estimatedRowCount.orElse(DEFAULT_ROW_COUNT);

        System.out.println("Import table " + tableInfo.sourceTable);

        BenchmarkResults results = new BenchmarkResults();
        results.tableInfo = tableInfo;
        results.rowCount = rowCount;
        tableImporter.startImport();
        results.testDurationMillis =
                measureExecutionTime(
                        () -> {
                            tableImporter.importPage();
                        });

        System.out.println(fileHelper.toJson(results));
    }

    private long measureExecutionTime(Runnable runnable) throws Exception {
        long start = System.currentTimeMillis();
        runnable.run();
        return System.currentTimeMillis() - start;
    }
}