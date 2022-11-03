package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core2.ConnectionParameters;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.example.snowflakecritic.DataSourceBuilder.*;
import static com.example.snowflakecritic.SnowflakeConnectorTestUtil.*;
import static com.example.snowflakecritic.SnowflakeSimpleTableImporterSpec.USE_SCHEMA_PUBLIC;
import static com.example.snowflakecritic.SnowflakeTableSyncStatus.NOT_STARTED;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowflakeIbfTableUpdaterSpec extends TestBase {

    private SnowflakeTableInfo testTableInfo = allTypesTestTable("PUBLIC", "TABLE1", "prefix");
    //private SnowflakeSourceCredentials testCredentials = testCredentials(SnowflakeSourceCredentials.UpdateMethod.IBF);
    private ConnectionParameters testConnectionParameters = testConnectionParams();
    private DataSourceBuilder dataSourceBuilder;

    private IbfCheckpointManager mockCheckpointManager;

    private String tableQuery;

    @Before
    public void init() {
        //String workingDir = "/home/baggage/projects/jsonz/v2/oracle-api-build/api-build/src/com/example/ibf/resources";
        //System.setProperty("work.dir", workingDir);

        testContext
                .setSnowflakeInformer(config -> mockInformer(testTableInfo))
                .withMockOutput()
                .withMockStandardConfig();

        // import should be completed
        SnowflakeConnectorState.SnowflakeTableState tableState = new SnowflakeConnectorState.SnowflakeTableState();
        // import is not done yet
        tableState.importStatus = NOT_STARTED;
        testContext.getConnectorState().tableStates.put(testTableInfo.sourceTable, tableState);

        tableQuery = SnowflakeSQLUtils.select(testTableInfo.includedColumnInfo(), testTableInfo.sourceTable.name);

        SnowflakeConnectorServiceConfiguration configuration = new SnowflakeConnectorServiceConfiguration();
        mockCheckpointManager = mock(IbfCheckpointManager.class);
        configuration.getIbfCheckpointManager(testTableInfo.sourceTable, mockCheckpointManager);
    }

    @Test
    public void incrementalUpdateHappyPath() {
        testContext.setSnowflakeSource(
                new DataSourceBuilder()
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
                                rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))))
                        .build());

        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.incrementalUpdate(tableRefs);

        System.out.println("Updates completed");
        /*MockOutput2<SnowflakeConnectorState> mockOutput = (MockOutput2) testContext.getOutput();
        List<Map<String, Object>> results = mockOutput.getAll(testTableInfo.destinationTableRef);
        for (Map<String, Object> row : results) {
            Object id = row.get(ID);
            assertNotNull(id);
            assertTrue(id instanceof Integer);
        }*/
    }

    @Test
    public void incrementalUpdateForImportStatusToTrue() {
        testContext.setSnowflakeSource(
                new DataSourceBuilder()
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
                                rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))))
                        .build());

        setupMockIbfCheckpointManager(true);
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).importStatus = SnowflakeTableSyncStatus.COMPLETE;
        incrementalUpdater.incrementalUpdate(tableRefs);

        assertEquals(SnowflakeTableSyncStatus.COMPLETE,incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).incUpdateStatus);
    }

    @Test
    public void incrementalUpdateForImportStatusToTrueAndFailed() {
        testContext.setSnowflakeSource(
                new DataSourceBuilder()
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
                                rows(row(Integer.valueOf(1), Long.valueOf(2L), Long.valueOf(3L), Long.valueOf(4L))))
                        .build());

        setupMockIbfCheckpointManager(false);
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).importStatus = SnowflakeTableSyncStatus.COMPLETE;
        incrementalUpdater.incrementalUpdate(tableRefs);

        assertEquals(SnowflakeTableSyncStatus.FAILED,incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).incUpdateStatus);
    }

    @Test
    public void incrementalUpdateException() {
        testContext.setSnowflakeSource(
                new DataSourceBuilder()
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
                                rows(row(1, 2L, 3L, 4L)))
                        .build());

        setupMockIbfCheckPointManagerException();
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        Set<TableRef> tableRefs = new HashSet<>();
        tableRefs.add(testTableInfo.sourceTable);
        incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).importStatus = SnowflakeTableSyncStatus.COMPLETE;
        incrementalUpdater.incrementalUpdate(tableRefs);

        assertEquals(SnowflakeTableSyncStatus.FAILED,incrementalUpdater.getSnowflakeConnectorState().getTableState(testTableInfo.sourceTable).incUpdateStatus);
    }

    @Test
    public void incrementalUpdateHandlingException() {
        // Test for Exception Handling with incorrect value.
        try {
            testContext.setSnowflakeSource(
                    new DataSourceBuilder()
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
                                    rows(row(1L, 2L, 3L, 4L)))
                            .build());

            SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
            Set<TableRef> tableRefs = new HashSet<>();
            tableRefs.add(testTableInfo.sourceTable);
            incrementalUpdater.incrementalUpdate(tableRefs);
        }
        catch(Exception e) {
            Assert.assertNull(e);
        }
    }

    private SnowflakeSourceCredentials testCredentials() {
        SnowflakeSourceCredentials credentials = new SnowflakeSourceCredentials();
        credentials.updateMethod = SnowflakeSourceCredentials.UpdateMethod.IBF;
        credentials.port = 0;
        credentials.database = Optional.of("dummy");
        return credentials;
    }

    private void setupMockIbfCheckpointManager(
            boolean succeeded) {
        IbfSyncResult syncResult = mock(IbfSyncResult.class);
        when(syncResult.getSucceeded()).thenReturn(succeeded);
        try {
            when(mockCheckpointManager.diff()).thenReturn(syncResult);
        } catch (Exception ignore) {
        }
    }

    private void setupMockIbfCheckPointManagerException() {
        try {
            when(mockCheckpointManager.diff()).thenReturn(null);
        } catch (Exception e) {

        }
    }

    @Test
    public void ibfPreTableImportHandler_exception() {
        // TODO test the exception path of IbfPreTableImportHandler: throw a SQLException from
        // getIbfCheckpointManager(tableRef).reset()
    }

    @Test
    public void checkpointManager_diffException() {
        // TODO test the condition where IbfSyncResult result = getIbfCheckpointManager().diff();
        // and result.getSucceeded() is false
    }

    @Test
    public void checkpointManager_updateException() {
        // TODO test the condition where getIbfCheckpointManager().update() throws an exception
    }

    @Test
    public void update_insertRecod() {

    }

    @Test
    public void update_updateRecord() {

    }

    @Test
    public void delete_removeRecord() {

    }

}
