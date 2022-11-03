package com.example.snowflakecritic;

import com.example.core.mocks.MockOutput2;
import com.example.core2.ConnectionParameters;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Optional;
import java.util.TimeZone;

import static com.example.snowflakecritic.DataSourceBuilder.*;
import static com.example.snowflakecritic.SnowflakeConnectorTestUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SnowflakeSimpleTableImporterSpec extends TestBase {

    public static final String USE_SCHEMA_PUBLIC = "USE SCHEMA PUBLIC";

    private SnowflakeTableInfo testTableInfo = allTypesTestTable("PUBLIC", "TABLE1", "prefix");

    private String tableQuery;
    IbfCheckpointManager mockCheckpointManager;

    @Before
    public void init() {

        String workingDir = "/home/baggage/projects/jsonz/v2/oracle-api-build/api-build/src/com/example/ibf/resources";

        // supply with your working dir
//        String workingDir = "/Users/j/Desktop/qx/oracle-api-build/api-build/src/com/example/snowflakecritic/scripts";

        System.setProperty("work.dir", workingDir);

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
    public void importPage_happyPath() {
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

        SnowflakeImporter importer = testContext.getImporter();
        // TODO check these 3 values - what is the best place to set them
        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
        importer.importPage(testTableInfo.sourceTable);
        assertTrue(importer.importFinished(testTableInfo.sourceTable));

        MockOutput2<SnowflakeConnectorState> mockOutput = (MockOutput2) testContext.getOutput();
//        List<Map<String, Object>> results = mockOutput.getAll(testTableInfo.destinationTableRef);
//        assertEquals(2, results.size());
//        for (Map<String, Object> row : results) {
//            Object id = row.get(ID);
//            assertNotNull(id);
//            assertTrue(id instanceof Integer);
//        }
    }

    @Test (expected = AssertionError.class)
    public void importPage_exception() {
        testContext
                .withMockStandardConfig()
                .setSnowflakeSource(
                        new DataSourceBuilder()
                                .sourceCredentials(testCredentials())
                                .params(testConnectionParams())
                                .execute(USE_SCHEMA_PUBLIC, new SQLException("test exception"))
                                .build());

        SnowflakeImporter importer = testContext.getImporter();
        importer.serviceConfig.setSnowflakeSource(testContext.getSource());
        importer.serviceConfig.setCredentials(testCredentials());
        importer.serviceConfig.setConnectionParameters(testConnectionParams());
        importer.serviceConfig.setStandardConfig(importer.getSnowflakeInformer().getStandardConfig());

        assertFalse(importer.importStarted(testTableInfo.sourceTable));
        importer.beforeImport(testTableInfo.sourceTable);
        assertTrue(importer.importStarted(testTableInfo.sourceTable));
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

