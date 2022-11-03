package com.example.snowflakecritic.scripts;

import com.example.core.ColumnType;
import com.example.core.StandardConfig;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.warning.Warning;
import com.example.core2.OperationTag;
import com.example.core2.Output;
import com.example.snowflakecritic.SnowflakeColumnInfo;
import com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeInformationSchemaDao;
import com.example.snowflakecritic.SnowflakeInformer;
import com.example.snowflakecritic.SnowflakeReplicationTableUpdater;
import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeSystemInfo;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.SnowflakeTimeTravelTableImporter;
import com.example.snowflakecritic.SnowflakeTimeTravelTableUpdater;
import com.example.snowflakecritic.SnowflakeType;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.example.snowflakecritic.scripts.SnowflakeConnectorScriptRunner.DEFAULT_ROW_COUNT;

public class TimeTravelBenchmarkRunner extends BaseBenchmarkRunner {

    public TimeTravelBenchmarkRunner(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    protected void runBenchmarkTest(SnowflakeTableInfo tableInfo) throws Exception {
        Object selectedOption =
                choose(
                        "change mode",
                        new Object[] {
                                new UpdateOption("Incremental sync - mixed", 0.33f, 0.33f, 0.33f),
                                new UpdateOption("Incremental sync - insert only", 1.0f, 0, 0),
                                new UpdateOption("Incremental sync - delete only", 0, 1.0f, 0),
                                new UpdateOption("Incremental sync - update only", 0, 0, 1.0f),
                                "Full sync"
                        });

        long rowCount = tableInfo.estimatedRowCount.orElse(DEFAULT_ROW_COUNT);
        Double changePercent = null;
        Supplier<Set<String>> benchmarkTest = null;
        SnowflakeConnectorServiceConfiguration configuration =
                new SnowflakeConnectorServiceConfiguration()
                        .setCredentials(source.originalCredentials)
                        .setStandardConfig(new StandardConfig())
                        .setSnowflakeSource(source)
                        .setConnectorState(new SnowflakeConnectorState());

        if ("Full sync".equals(selectedOption)) {
            benchmarkTest = runImporterBenchmark(configuration, tableInfo);
        } else {
            changePercent = choose("change percent", new Double[] {0.0005, 0.005, 0.05});
            benchmarkTest =
                    runUpdaterBenchmark(
                            configuration, tableInfo, (UpdateOption) selectedOption, rowCount, changePercent);
        }

        BenchmarkResults results = new BenchmarkResults();
        if (changePercent != null) {
            results.updatePercent = changePercent;
        }
        results.tableInfo = tableInfo;
        results.rowCount = rowCount;
        results.testDurationMillis = measureExecutionTime(benchmarkTest);
        System.out.println(fileHelper.toJson(results));
    }

    public void setOutput(Output output) {
        configuration.setOutput(output);
    }

    private Supplier<Set<String>> runImporterBenchmark(
            SnowflakeConnectorServiceConfiguration configuration, SnowflakeTableInfo tableInfo) {
        System.out.println("Import table " + tableInfo.sourceTable);
        SnowflakeTimeTravelTableImporter tableImporter =
                new SnowflakeTimeTravelTableImporter(configuration, tableInfo.sourceTable);
        return () -> {
            tableImporter.startImport();
            tableImporter.importPage();
            return tableImporter.queryIDs();
        };
    }

    private Supplier<Set<String>> runUpdaterBenchmark(
            SnowflakeConnectorServiceConfiguration configuration,
            SnowflakeTableInfo tableInfo,
            UpdateOption updateOption,
            long rowCount,
            double changePercent) {
        SnowflakeTableInfo clonedTableInfo = clone(tableInfo);
        System.out.println("Update table " + clonedTableInfo.sourceTable);
        SnowflakeTimeTravelTableUpdater tableUpdater =
                new SnowflakeTimeTravelTableUpdater(configuration, clonedTableInfo.sourceTable);
        SnowflakeConnectorState.SnowflakeTableState tableState =
                configuration.getConnectorState().getTableState(clonedTableInfo.sourceTable);
        SnowflakeSystemInfo snowflakeSystemInfo = new SnowflakeSystemInfo(source);
        tableState.lastSyncTime = snowflakeSystemInfo.getSystemTime();
        new DatabaseRandomChangeGenerator(clonedTableInfo, source, updateOption)
                .generate((int) (rowCount * changePercent), tableInfo.sourceTable.name, Optional.of(1l));
        return () -> {
            tableUpdater.incrementalUpdate();
            return tableUpdater.queryIDs();
        };
    }

    private long measureExecutionTime(Supplier<Set<String>> supplier) {
        return source.execute(
                resultSet -> {
                    long totalElapsedTime = 0;
                    while (resultSet.next()) {
                        long elapsedTime = resultSet.getLong("TOTAL_ELAPSED_TIME");
                        String queryId = resultSet.getString("QUERY_ID");
                        System.out.println("Query: " + queryId + " costs " + elapsedTime + "ms");
                        totalElapsedTime += elapsedTime;
                    }
                    return totalElapsedTime;
                },
                "select query_id, total_elapsed_time from table(information_schema.query_history()) where query_id in (%s)",
                supplier.get().stream().map(query_id -> "'" + query_id + "'").collect(Collectors.joining(",")));
    }

    private SnowflakeTableInfo clone(SnowflakeTableInfo tableInfo) {
        System.out.println("Clone " + tableInfo.sourceTable);
        String cloneTableName = String.format("%s_CLONE", tableInfo.sourceTable.name);
        source.update("create or replace table %s clone %s", cloneTableName, tableInfo.sourceTable.name);
        source.update("alter table %s set change_tracking = true", cloneTableName);

        // TODO a little bit inefficient that to load all table info to find one..
        SnowflakeInformer informer = configuration.getSnowflakeInformer();
        SnowflakeInformationSchemaDao informationSchemaDao = informer.newInformationSchemaDao();
        Map<TableRef, SnowflakeTableInfo> tables = informationSchemaDao.fetchTableInfo();
        return tables.get(new TableRef(tableInfo.sourceTable.schema, cloneTableName));
    }

    private static class DatabaseRandomChangeGenerator {
        private SnowflakeTableInfo tableInfo;
        private SnowflakeSource source;
        private UpdateOption updateOption;

        public DatabaseRandomChangeGenerator(
                SnowflakeTableInfo tableInfo, SnowflakeSource source, UpdateOption updateOption) {
            this.tableInfo = tableInfo;
            this.source = source;
            this.updateOption = updateOption;
        }

        public void generate(int changeSize, String seedTableName, Optional<Long> maxReplicationKey) {
            int newRandomDatabaseRecordSize = (int) (changeSize * updateOption.changePercentForInserts);
            int deleteRandomDatabaseRecordSize = (int) (changeSize * updateOption.changePercentForDeletes);
            int updateRandomDatabaseRecordSize = (int) (changeSize * updateOption.changePercentForUpdates);
            Iterator<DatabaseRandomChange> changes =
                    IteratorUtils.chainedIterator(
                            new Iterator[] {
                                    new SupplierIterator(
                                            () -> new NewRandomDatabaseRecord(tableInfo), newRandomDatabaseRecordSize),
                                    new SupplierIterator(
                                            () ->
                                                    new DeleteRandomDatabaseRecord(
                                                            tableInfo, tableInfo.estimatedRowCount.orElse(100l)),
                                            deleteRandomDatabaseRecordSize),
                                    new SupplierIterator(
                                            () ->
                                                    new UpdateRandomDatabaseRecord(
                                                            tableInfo, tableInfo.estimatedRowCount.orElse(100l)),
                                            updateRandomDatabaseRecordSize)
                            });

            try (Connection connection = source.connection();
                 Statement statement = connection.createStatement(); ) {
                connection.setAutoCommit(false);

                while (changes.hasNext()) {
                    changes.next().apply(statement);
                }
                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SupplierIterator<E> implements Iterator<E> {
        private Supplier<E> supplier;
        private int count;
        private int index;

        public SupplierIterator(Supplier<E> supplier, int count) {
            this.supplier = supplier;
            this.count = count;
        }

        @Override
        public boolean hasNext() {
            return index < count;
        }

        @Override
        public E next() {
            index++;
            return supplier.get();
        }

        @Override
        public void remove() {}

        @Override
        public void forEachRemaining(Consumer<? super E> action) {}
    }

    private abstract static class DatabaseRandomChange {
        protected SnowflakeTableInfo tableInfo;

        public DatabaseRandomChange(SnowflakeTableInfo tableInfo) {
            this.tableInfo = tableInfo;
        }

        protected abstract String sql();

        public void apply(Statement statement) throws SQLException {
            String changeSql = sql();
            System.out.println("####### apply random change: " + changeSql);
            statement.addBatch(changeSql);
        }

        protected String randomValue(SnowflakeColumnInfo columnInfo) {
            return random(columnInfo.sourceType, columnInfo);
        }

        protected String wrapSqlValue(Object value) {
            if (value == null) {
                return "null";
            }

            if (value instanceof String) {
                return "'" + value + "'";
            }
            return String.valueOf(value);
        }
    }

    private static class NewRandomDatabaseRecord extends DatabaseRandomChange {
        public NewRandomDatabaseRecord(SnowflakeTableInfo tableInfo) {
            super(tableInfo);
        }

        @Override
        protected String sql() {
            return String.format(
                    "insert into %s(%s) values(%s)",
                    tableInfo.sourceTable.name,
                    tableInfo
                            .sourceColumnInfo()
                            .stream()
                            .map(column -> column.columnName)
                            .collect(Collectors.joining(",")),
                    tableInfo
                            .sourceColumnInfo()
                            .stream()
                            .map(column -> randomValue(column))
                            .collect(Collectors.joining(",")));
        }
    }

    private static class DeleteRandomDatabaseRecord extends DatabaseRandomChange {
        private long rowCount;

        public DeleteRandomDatabaseRecord(SnowflakeTableInfo tableInfo, long rowCount) {
            super(tableInfo);
            this.rowCount = rowCount;
        }

        @Override
        protected String sql() {
            return String.format(
                    "delete from %s where id = %d", tableInfo.sourceTable.name, RandomUtils.nextLong(1, rowCount + 1));
        }
    }

    private static class UpdateRandomDatabaseRecord extends DatabaseRandomChange {
        private long rowCount;

        public UpdateRandomDatabaseRecord(SnowflakeTableInfo tableInfo, long rowCount) {
            super(tableInfo);
            this.rowCount = rowCount;
        }

        @Override
        protected String sql() {
            Supplier<Stream<SnowflakeColumnInfo>> columns =
                    () ->
                            tableInfo
                                    .sourceColumnInfo()
                                    .stream()
                                    .filter(
                                            column ->
                                                    (column.sourceType != SnowflakeType.DATE)
                                                            && (column.sourceType != SnowflakeType.TIMESTAMP_LTZ)
                                                            && (column.sourceType != SnowflakeType.TIMESTAMP_NTZ)
                                                            && (column.sourceType != SnowflakeType.TIMESTAMP_LTZ));
            return String.format(
                    "update %s set %s where id = %d",
                    tableInfo.sourceTable.name,
                    columns.get()
                            .map(column -> column.columnName + " = " + randomValue(column))
                            .collect(Collectors.joining(",")),
                    RandomUtils.nextLong(1, rowCount + 1));
        }
    }

    private static class UpdateOption {
        protected String description;
        protected double changePercentForInserts;
        protected double changePercentForDeletes;
        protected double changePercentForUpdates;

        public UpdateOption(
                String description,
                double changePercentForInserts,
                double changePercentForDeletes,
                double changePercentForUpdates) {
            this.description = description;
            this.changePercentForInserts = changePercentForInserts;
            this.changePercentForDeletes = changePercentForDeletes;
            this.changePercentForUpdates = changePercentForUpdates;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private <T> T choose(String label, T[] options) throws IOException {
        System.out.println(
                String.format("\n============================================\nChoose %s or 0 to exit: ", label));
        for (int i = 0; i < options.length; i++) {
            System.out.println(String.format("%d. %s", i + 1, options[i].toString()));
        }
        int choice = getUserChoice(options.length);
        if (choice == 0) {
            throw new RuntimeException("Cancelling test");
        }
        return options[choice - 1];
    }

    public static Output<SnowflakeConnectorState> getScriptOutput() {
        return new Output<SnowflakeConnectorState>() {
            //@Override
            public TableDefinition promoteColumnType(
                    TableDefinition tableDefinition, String columnName, ColumnType newType) {
                return null;
            }

//            @Override
//            public TableDefinition updateSchema(TableDefinition table, UpdateSchemaOperation updateSchema) {
//                return null;
//            }

            @Override
            public void upsert(TableDefinition table, Object value, OperationTag... tags) {}

            @Override
            public void upsert(TableDefinition table, Object value, Instant operationTimestamp, OperationTag... tags) {}

            @Override
            public void update(TableDefinition table, Object diff, OperationTag... tags) {}

            @Override
            public void update(TableDefinition table, Object diff, Instant operationTimestamp, OperationTag... tags) {}

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

//            @Override
//            public void signal(SignalType signalType, TableRef... tableRefs) {}
//
//            @Override
//            public RenamingFilter2 renamingFilter() {
//                return null;
//            }

            @Override
            public void warn(Warning warning) {}

            @Override
            public void close() {}
        };
    }

    public static String random(SnowflakeType type, SnowflakeColumnInfo columnInfo) {
        switch (type) {
            case NUMBER:
                return String.valueOf(
                        RandomUtils.nextLong(
                                0,
                                (long)
                                        Math.pow(
                                                10,
                                                columnInfo.numericPrecision.orElse(3)
                                                        - columnInfo.numericScale.orElse(0))));
            case FLOAT:
                return String.valueOf(
                        RandomUtils.nextFloat(
                                0,
                                (float)
                                        Math.pow(
                                                10,
                                                columnInfo.numericPrecision.orElse(3)
                                                        - columnInfo.numericScale.orElse(0))));
            case TEXT:
                return "'" + RandomStringUtils.randomAlphanumeric(columnInfo.getMaxLength().orElse(10)) + "'";
            case DATE:
                return String.format(
                        "to_date('%s')",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd")
                                .withZone(ZoneId.systemDefault())
                                .format(
                                        Instant.ofEpochSecond(
                                                RandomUtils.nextLong(
                                                        Instant.now().getEpochSecond(),
                                                        Instant.now().plus(Duration.ofDays(100)).getEpochSecond()))));
            case BOOLEAN:
            case TIMESTAMP_NTZ:
            case TIMESTAMP_TZ:
            case TIMESTAMP_LTZ:
            case VARIANT:
            case BINARY:
                return "null";
            default:
                throw new IllegalArgumentException("unrecognized type: " + type);
        }
    }
}
