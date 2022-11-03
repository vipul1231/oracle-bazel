package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.example.core2.ConnectionParameters;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.example.snowflakecritic.DataSourceBuilder.*;
import static com.example.snowflakecritic.SnowflakeConnectorTestUtil.*;
import static com.example.snowflakecritic.SnowflakeSimpleTableImporterSpec.USE_SCHEMA_PUBLIC;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for the snowflake update process
 * How incremental updater works after importer is finished
 */
public class SnowflakeUpdateScenarioSpec extends TestBase {
    private SnowflakeTableInfo testTableInfo = allTypesTestTable("PUBLIC", "TABLE1", "prefix");
    private ConnectionParameters testConnectionParameters = testConnectionParams();
    private DataSourceBuilder dataSourceBuilder;

    private String tableQuery;
    IbfCheckpointManager mockCheckpointManager;

    DataSourceBuilder builder;

    @Before
    public void init() {
        // TODO - this has to be changed accordingly
        //String workingDir = "/home/baggage/projects/jsonz/v2/oracle-api-build/api-build/src/com/example/ibf/resources";
        //System.setProperty("work.dir", workingDir);

        testContext
                .setConnectionParameters(testConnectionParams())
                .setCredentials(testCredentials())
                .setSnowflakeInformer(config -> mockInformer(testTableInfo))
                .withMockOutput()
                .withMockStandardConfig();
        tableQuery = SnowflakeSQLUtils.select(testTableInfo.includedColumnInfo(), testTableInfo.sourceTable.name);

        SnowflakeConnectorServiceConfiguration configuration = new SnowflakeConnectorServiceConfiguration();
        mockCheckpointManager = mock(IbfCheckpointManager.class);
        configuration.getIbfCheckpointManager(testTableInfo.sourceTable, mockCheckpointManager);
    }

    @Test
    public void sqlUtilWhereInTest() {
        List<String> columns = new ArrayList<>();
        columns.add("ID");

        List<List<Object>> keys = ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2));
        String whereClause = SnowflakeSQLUtils.whereIn(columns, keys);
        // query is successfull
        Assert.assertNotNull(whereClause);
        Assert.assertTrue(whereClause.contains("IN (1, 2)"));
    }

    @Test
    public void insertAfterImportCompleted() {
        builder = new DataSourceBuilder()
                .sourceCredentials(testCredentials())
                .params(testConnectionParams())
                .execute(USE_SCHEMA_PUBLIC, true)
                .executeQuery(
                        tableQuery,
                        columns(
                                ID,
                                DATE_COL,
                                TEXT_COL,
                                TIME_COL,
                                FLOAT_COL,
                                TIMESTAMP_NTZ_COL,
                                TIMESTAMP_TZ_COL,
                                TIMESTAMP_LTZ_COL,
                                BINARY_COL,
                                BOOLEAN_COL),
                        rows(
                                row(
                                        number(1),
                                        date("2022-12-31"),
                                        "test 1",
                                        time("01:00:01"),
                                        number(0.1),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCD"),
                                        Boolean.TRUE),
                                row(
                                        number(1),
                                        date("1984-01-01"),
                                        "test 2",
                                        time("23:00:02"),
                                        number(0.2),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCDEF"),
                                        Boolean.FALSE)))
                .executePreparedStatementQuery(
                        columns(
                                "cellIndex",
                                "_ibf_key_hash",
                                "_ibf_column$0",
                                "count(_ibf_row_hash)"),
                        rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))));
        testContext.setSnowflakeSource(
                builder.build());

        SnowflakeImporter importer = testContext.getImporter();

        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
        importer.importPage(testTableInfo.sourceTable);
        assertTrue(importer.importFinished(testTableInfo.sourceTable));

        // do insert now (3 records)
        List<List<Object>> keys = ImmutableList.of(ImmutableList.of(3), ImmutableList.of(4), ImmutableList.of(5));
        setupMockIbfCheckpointManager(
                true,
                keys, new HashSet<>());
        builder.executePreparedStatementQuery(columns(
                ID,
                DATE_COL,
                TEXT_COL,
                TIME_COL,
                FLOAT_COL,
                TIMESTAMP_NTZ_COL,
                TIMESTAMP_TZ_COL,
                TIMESTAMP_LTZ_COL,
                BINARY_COL,
                BOOLEAN_COL),
                rows(
                        row(
                                number(3),
                                date("2022-12-31"),
                                "test 1",
                                time("01:00:01"),
                                number(0.1),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCD"),
                                Boolean.TRUE),
                        row(
                                number(4),
                                date("1984-01-01"),
                                "test 2",
                                time("23:00:02"),
                                number(0.2),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCDEF"),
                                Boolean.FALSE),
                        row(
                                number(5),
                                date("1990-01-01"),
                                "test 3",
                                time("23:00:02"),
                                number(0.2),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCDEFG"),
                                Boolean.FALSE)));

        // now we do an update
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        incrementalUpdater.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.incrementalUpdate(tableRefs);

        // here we have to assert the output
        Assert.assertEquals(testContext.getOutput().getClass(), MockOutput2.class);
        MockOutput2 output = (MockOutput2) testContext.getOutput();

        // we have inserted 3 records after import. So we mock that.
        MockOutput2.Counter counter = output.getCounter(testTableInfo.sourceTable);
        Assert.assertEquals(counter.getUpdateCounter(), 3);
    }


    @Test
    public void deleteOneAfterImportCompleted() {
        builder = new DataSourceBuilder()
                .sourceCredentials(testCredentials())
                .params(testConnectionParams())
                .execute(USE_SCHEMA_PUBLIC, true)
                .executeQuery(
                        tableQuery,
                        columns(
                                ID,
                                DATE_COL,
                                TEXT_COL,
                                TIME_COL,
                                FLOAT_COL,
                                TIMESTAMP_NTZ_COL,
                                TIMESTAMP_TZ_COL,
                                TIMESTAMP_LTZ_COL,
                                BINARY_COL,
                                BOOLEAN_COL),
                        rows(
                                row(
                                        number(1),
                                        date("2022-12-31"),
                                        "test 1",
                                        time("01:00:01"),
                                        number(0.1),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCD"),
                                        Boolean.TRUE),
                                row(
                                        number(2),
                                        date("1984-01-01"),
                                        "test 2",
                                        time("23:00:02"),
                                        number(0.2),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCDEF"),
                                        Boolean.FALSE)))
                .executePreparedStatementQuery(
                        columns(
                                "cellIndex",
                                "_ibf_key_hash",
                                "_ibf_column$0",
                                "count(_ibf_row_hash)"),
                        rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))));
        testContext.setSnowflakeSource(
                builder.build());

        SnowflakeImporter importer = testContext.getImporter();

        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
        importer.importPage(testTableInfo.sourceTable);
        assertTrue(importer.importFinished(testTableInfo.sourceTable));

        // now we delete 2 records
        List l1 = new ArrayList();
        l1.add("1");
        l1.add("2");
        Set set = new HashSet();
        set.add(l1);
        setupMockIbfCheckpointManager(
                true,
                new ArrayList<>(), set);
        builder.executePreparedStatementQuery(columns(
                ID,
                DATE_COL,
                TEXT_COL,
                TIME_COL,
                FLOAT_COL,
                TIMESTAMP_NTZ_COL,
                TIMESTAMP_TZ_COL,
                TIMESTAMP_LTZ_COL,
                BINARY_COL,
                BOOLEAN_COL),
                rows(
                        row(
                                number(1),
                                date("2022-12-31"),
                                "test 1",
                                time("01:00:01"),
                                number(0.1),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCD"),
                                Boolean.TRUE)
                ));

        // now we do an update
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        incrementalUpdater.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.incrementalUpdate(tableRefs);

        // here we have to assert the output
        Assert.assertEquals(testContext.getOutput().getClass(), MockOutput2.class);
        MockOutput2 output = (MockOutput2) testContext.getOutput();

        // we have deleted 1 record after import. So we mock that.
        MockOutput2.Counter counter = output.getCounter(testTableInfo.sourceTable);

        // assert zero since no record will be left as we inserted only 2 records and both are deleted.
        Assert.assertEquals(counter.getDeleteCounter(), 0);
    }

    @Test
    public void updateAfterImportCompleted() {
        builder = new DataSourceBuilder()
                .sourceCredentials(testCredentials())
                .params(testConnectionParams())
                .execute(USE_SCHEMA_PUBLIC, true)
                .executeQuery(
                        tableQuery,
                        columns(
                                ID,
                                DATE_COL,
                                TEXT_COL,
                                TIME_COL,
                                FLOAT_COL,
                                TIMESTAMP_NTZ_COL,
                                TIMESTAMP_TZ_COL,
                                TIMESTAMP_LTZ_COL,
                                BINARY_COL,
                                BOOLEAN_COL),
                        rows(
                                row(
                                        number(1),
                                        date("2022-12-31"),
                                        "test 1",
                                        time("01:00:01"),
                                        number(0.1),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCD"),
                                        Boolean.TRUE),
                                row(
                                        number(1),
                                        date("1984-01-01"),
                                        "test 2",
                                        time("23:00:02"),
                                        number(0.2),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCDEF"),
                                        Boolean.FALSE)))
                .executePreparedStatementQuery(
                        columns(
                                "cellIndex",
                                "_ibf_key_hash",
                                "_ibf_column$0",
                                "count(_ibf_row_hash)"),
                        rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))));
        testContext.setSnowflakeSource(
                builder.build());

        SnowflakeImporter importer = testContext.getImporter();

        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
        importer.importPage(testTableInfo.sourceTable);
        assertTrue(importer.importFinished(testTableInfo.sourceTable));

        // do update now (1 record)
        List<List<Object>> keys = ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2));
        setupMockIbfCheckpointManager(
                true,
                keys, new HashSet<>());
        builder.executePreparedStatementQuery(columns(
                ID,
                DATE_COL,
                TEXT_COL,
                TIME_COL,
                FLOAT_COL,
                TIMESTAMP_NTZ_COL,
                TIMESTAMP_TZ_COL,
                TIMESTAMP_LTZ_COL,
                BINARY_COL,
                BOOLEAN_COL),
                rows(
                        row(
                                number(2),
                                date("2022-12-31"),
                                "test 1",
                                time("01:00:01"),
                                number(0.1),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCD"),
                                Boolean.TRUE)));

        // now we do an update
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        incrementalUpdater.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.incrementalUpdate(tableRefs);

        // here we have to assert the output
        Assert.assertEquals(testContext.getOutput().getClass(), MockOutput2.class);
        MockOutput2 output = (MockOutput2) testContext.getOutput();

        // we have updated 1 record after import. So we mock that.
        MockOutput2.Counter counter = output.getCounter(testTableInfo.sourceTable);
        Assert.assertEquals(counter.getUpdateCounter(), 1);
    }

    @Test
    public void noInsertForFailedSucceedFlag() {
        builder = new DataSourceBuilder()
                .sourceCredentials(testCredentials())
                .params(testConnectionParams())
                .execute(USE_SCHEMA_PUBLIC, true)
                .executeQuery(
                        tableQuery,
                        columns(
                                ID,
                                DATE_COL,
                                TEXT_COL,
                                TIME_COL,
                                FLOAT_COL,
                                TIMESTAMP_NTZ_COL,
                                TIMESTAMP_TZ_COL,
                                TIMESTAMP_LTZ_COL,
                                BINARY_COL,
                                BOOLEAN_COL),
                        rows(
                                row(
                                        number(1),
                                        date("2022-12-31"),
                                        "test 1",
                                        time("01:00:01"),
                                        number(0.1),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCD"),
                                        Boolean.TRUE),
                                row(
                                        number(2),
                                        date("1984-01-01"),
                                        "test 2",
                                        time("23:00:02"),
                                        number(0.2),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        timestamp("2022-01-01 01:00:01"),
                                        binary("ABCDEF"),
                                        Boolean.FALSE)))
                .executePreparedStatementQuery(
                        columns(
                                "cellIndex",
                                "_ibf_key_hash",
                                "_ibf_column$0",
                                "count(_ibf_row_hash)"),
                        rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))));
        testContext.setSnowflakeSource(
                builder.build());

        SnowflakeImporter importer = testContext.getImporter();

        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
        importer.importPage(testTableInfo.sourceTable);
        assertTrue(importer.importFinished(testTableInfo.sourceTable));

        setupMockIbfCheckpointManager(
                false,
                new ArrayList<>(), new HashSet<>());

        //Now insert one record
        builder.executePreparedStatementQuery(columns(
                ID,
                DATE_COL,
                TEXT_COL,
                TIME_COL,
                FLOAT_COL,
                TIMESTAMP_NTZ_COL,
                TIMESTAMP_TZ_COL,
                TIMESTAMP_LTZ_COL,
                BINARY_COL,
                BOOLEAN_COL),
                rows(
                        row(
                                number(1),
                                date("2022-12-31"),
                                "test 1",
                                time("01:00:01"),
                                number(0.1),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                timestamp("2022-01-01 01:00:01"),
                                binary("ABCD"),
                                Boolean.TRUE)));

        // now we do an update
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        incrementalUpdater.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.incrementalUpdate(tableRefs);

        // here we have to assert the output
        MockOutput2 output = (MockOutput2) testContext.getOutput();

        // we have inserted 1 records after import. So we mock that.
        MockOutput2.Counter counter = output.getCounter(testTableInfo.sourceTable);
        Assert.assertEquals(0, counter.getUpdateCounter());

        SnowflakeConnectorState.SnowflakeTableState tableState = incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable);
        assertEquals(SnowflakeTableSyncStatus.FAILED, tableState.incUpdateStatus);
    }

    private void setupMockIbfCheckpointManager(
            boolean succeeded, List<List<Object>> upserts, Set<List<Object>> deletes) {
        IbfSyncResult syncResult = mock(IbfSyncResult.class);
        when(syncResult.getSucceeded()).thenReturn(succeeded);
        when(syncResult.upserts()).thenReturn(upserts);
        when(syncResult.deletes()).thenReturn(deletes);

        try {
            when(mockCheckpointManager.diff()).thenReturn(syncResult);
        } catch (Exception ignore) {
        }
    }

    private SnowflakeSourceCredentials testCredentials() {
        SnowflakeSourceCredentials credentials = new SnowflakeSourceCredentials();
        credentials.updateMethod = SnowflakeSourceCredentials.UpdateMethod.IBF;
        credentials.port = 0;
        credentials.database = Optional.of("dummy");
        return credentials;
    }

    private ConnectionParameters testConnectionParams() {
        return new ConnectionParameters("owner", "prefix", TimeZone.getDefault());
    }
}
