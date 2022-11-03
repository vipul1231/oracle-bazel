package com.example.oracle;

import com.example.core.TableRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;

import static com.example.oracle.Constants.PAGE_SIZE_CONFIG_HOURS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OracleApiSpec extends AbstractOracleTestSpec{

    private static final TableRef MOCK_TABLE = new TableRef("S", "T");
    private static final Set<TableRef> SELECTED_TABLES = ImmutableSet.of(MOCK_TABLE);
//    @Rule public FeatureFlagHelper flagHelper = new FeatureFlagHelper();
    private OracleApi api;
    private SetupValidator setupValidator;

    @BeforeClass
    public static void beforeClassOracleApiSpec() {
//        TaskTypeRegistry.register(new OracleService(OracleServiceType.ORACLE, false).taskTypes());
        TimeZone.setDefault(TimeZone.getTimeZone("UTC")); // Matters for JDBC's interpretation of timestamps
    }

    @Before
    public void beforeOracleApiSpec() {
        dataSource = mock(DataSource.class);
        api = spy(new OracleApi(dataSource));

        // new method in OracleApi
//        when(api.newDbVersionSupplier(ArgumentMatchers.any(Connection.class))).thenReturn(() -> "12.0.0.0.0");

        Transactions.sleepDurationFunction = Transactions.quickSleepDurationFunction; // speed up tests with retry logic
//        api.pageSizeConfig = new Lazy<>(() -> new OracleDynamicPageSizeConfig(300_000L, 50_000L, 50_000L));

        setupValidator = spy(SetupValidator.class);
    }


    @Test
    public void RowPrimaryKeys_build_oneColumnNotChanged() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("static_pk"));

        Map<String, Object> row = new HashMap<>();
        row.put("static_pk", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> "1", row);
        assertFalse(rowPrimaryKeys.changed());
    }

    @Test
    public void RowPrimaryKeys_build_multiColumnNotChanged() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("static_pk"));
        columns.add(makePkColumn("static_pk2"));

        Map<String, Object> row = new HashMap<>();
        row.put("static_pk", "1");
        row.put("static_pk2", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> "1", row);
        assertFalse(rowPrimaryKeys.changed());
    }

    @Test
    public void RowPrimaryKeys_build_oneColumnChanged() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("variable_pk"));

        Map<String, Object> row = new HashMap<>();
        row.put("variable_pk", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> c, row);
        assertTrue(rowPrimaryKeys.changed());
    }

    @Test
    public void RowPrimaryKeys_build_multiColumnSomeChanged() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("variable_pk"));
        columns.add(makePkColumn("static_pk"));

        Map<String, Object> row = new HashMap<>();
        row.put("variable_pk", "1");
        row.put("static_pk", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> c.equals("opk0") ? c : "1", row);
        assertTrue(rowPrimaryKeys.changed());
    }

    @Test
    public void RowPrimaryKeys_build_multiColumnAllChanged() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("variable_pk"));
        columns.add(makePkColumn("variable_pk2"));

        Map<String, Object> row = new HashMap<>();
        row.put("variable_pk", "1");
        row.put("variable_pk2", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> c, row);
        assertTrue(rowPrimaryKeys.changed());
    }

    @Test
    public void RowPrimaryKeys_toString() throws SQLException {
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(makePkColumn("variable_pk"));
        columns.add(makePkColumn("variable_pk2"));

        Map<String, Object> row = new HashMap<>();
        row.put("variable_pk", "1");
        row.put("variable_pk2", "1");

        Transactions.RowPrimaryKeys rowPrimaryKeys = Transactions.RowPrimaryKeys.build(columns, (c) -> c, row);
        assertEquals(
                "RowPrimaryKeys {oldPrimaryKeys {variable_pk=opk0, variable_pk2=opk1}, newPrimaryKeys {variable_pk=1, variable_pk2=1}}",
                rowPrimaryKeys.toString());
    }

    @Test
    public void getPageSizeConfig() {
        assertPageSizeConfig(10);
        assertPageSizeConfig(100_000);
        assertPageSizeConfig(100_000_000);
        assertPageSizeConfig(5_398_334_932L);
        assertPageSizeConfig(Long.MAX_VALUE);
    }

    private void assertPageSizeConfig(long totalSCNs) {
        doReturn(1 + totalSCNs).when(api).mostRecentScn();
        doReturn(Optional.of(1L)).when(new SqlUtil()).scnXHoursBeforeScn(anyLong(), anyLong());

        OracleDynamicPageSizeConfig config = api.getPageSizeConfig();

        long scnsPerMinute = totalSCNs / (PAGE_SIZE_CONFIG_HOURS * 60);

        assertEquals("min", scnsPerMinute * 5, config.getMin());
        assertEquals("start", scnsPerMinute * 30, config.getStart());
        assertEquals("step", scnsPerMinute * 5, config.getStep());
        assertEquals("temp_extra_step", scnsPerMinute * 5 * 2, config.getTempExtraStep());
    }

    @Test
    public void getPageSizeConfig_defaultOnError() {
        doReturn(100_000L).when(api).mostRecentScn();
        doReturn(Optional.empty()).when(new SqlUtil()).scnXHoursBeforeScn(anyLong(), anyLong());

        OracleDynamicPageSizeConfig config = api.getPageSizeConfig();

        assertEquals(new OracleDynamicPageSizeConfig(), config);
    }

    @Test
    public void connectToSourceDb_reconnectOnFailure() throws SQLException {
        // prepare
        doThrow(new SQLException("TestException")).when(dataSource).getConnection();

        // execute
//        assertThrows(CompleteWithTask.class, () -> api.connectToSourceDb());
    }

    @Test
    public void hasTableAccess_returnFalseOnError() throws Exception {
        // prepare
        Connection mockConnection = mock(Connection.class);
//        api.connectionManagerWithRetry = mock(ConnectionManagerWithRetry.class);
//        doReturn(mockConnection).when(api.connectionManagerWithRetry).connectToSourceDb(anyBoolean());
        doThrow(new SQLException("TestException")).when(mockConnection).prepareStatement(anyString());

//        doCallRealMethod()
//                .when(api.connectionManagerWithRetry)
//                .retryWithFailureCounter(any(), any(), anyBoolean(), any(), any());
//        doCallRealMethod().when(api.connectionManagerWithRetry).retry(any(), any(), anyBoolean(), any());
//        doCallRealMethod().when(api.connectionManagerWithRetry).retry(any(), any());

        // execute
        boolean hasAccess = setupValidator.hasTableAccess("ALL_TABLES");

        // assert
        assertFalse(hasAccess);
    }

    @Test
    public void undoRetention_Fetch() throws SQLException {
        // Arrange
        ResultSet mockResultSet = mock(ResultSet.class);
        doReturn(true).when(mockResultSet).next();
        doReturn(100).when(mockResultSet).getInt(anyString());

        // Act
        int undoLogRetention = api.getUndoLogRetention(mockConnection(mockResultSet));

        // Assert
        assertEquals(100, undoLogRetention);
    }

    @Test
    public void undoRetention_ValueNotSet() throws SQLException {

        // Arrange
        ResultSet mockResultSet = mock(ResultSet.class);
        doReturn(false).when(mockResultSet).next();

        // Act
//        RuntimeException exception =
//                assertThrows(RuntimeException.class, () -> api.getUndoLogRetention(mockConnection(mockResultSet)));
//        assertEquals(exception.getMessage(), "No value set for UNDO_RETENTION");
    }

    @Test
    public void undoRetention_MissingSelectPermission() throws SQLException {

        // Arrange
        Connection connection = mockSqlExceptionConnection("...table or view does not exist...");

        // Act
//        RuntimeException exception = assertThrows(RuntimeException.class, () -> api.getUndoLogRetention(connection));
//        assertEquals(exception.getMessage(), "Missing SELECT permission for V$SYSTEM_PARAMETER");
    }

    @Test
    public void oracleApi_tablesLogMinerExclusions() throws SQLException {
        TableRef table1 = TableRef.parse("test_schema.TABLE_NAME");
        TableRef table2 = TableRef.parse("test_schema.MLOG$_TEST");
        TableRef table3 = TableRef.parse("test_schema.TABLE_NAME_THAT_EXCEEDS_30CHARS");
        TableRef table4 = TableRef.parse("test_schema.TABLE_WITH_LONG_COLUMN_NAME");

//        DataDictionaryDaoTestHelper queryHelper = new DataDictionaryDaoTestHelper();

        Statement mockStatement = mock(Statement.class);

//        ResultSet resultSet1 =
//                resultSetFrom(
//                        columns(TABLE_SCHEMA, TABLE_NAME),
//                        rows(
//                                row(table1.schema, table1.name),
//                                row(table2.schema, table2.name),
//                                row(table3.schema, table3.name),
//                                row(table4.schema, table4.name)));
//
//        doReturn(resultSet1).when(mockStatement).executeQuery(queryHelper.getAllTablesQuery());

//        ResultSet resultSet2 =
//                resultSetFrom(
//                        columns(TABLE_SCHEMA, TABLE_NAME,
//                                COLUMN_NAME),
//                        rows(row(table4.schema, table4.name, "COLUMN_NAME_THAT_EXCEEDS_30CHARS")));
//        doReturn(resultSet2)
//                .when(mockStatement)
//                .executeQuery(queryHelper.getTablesWithLongColumnNamesQuery(table1, table4));

        Connection conn = mockConnection(mockStatement);

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(conn);
        OracleApi api = spy(new OracleApi(dataSource));

//        api.connectionManagerWithRetry = spy(new ConnectionManagerWithRetry(dataSource, Optional.empty()));
//        api.flashbackConfigurationValidator = mock(FlashbackConfigurationValidator.class);
//        doReturn(false).when(api.flashbackConfigurationValidator).flashbackDataArchiveEnabled(any());
//        doReturn(false).when(api.flashbackConfigurationValidator).isTableFlashbackEnabled(any(), any());

        Map<TableRef, Optional<String>> tables = api.tables();

        assertTrue(tables.containsKey(table1));
        assertFalse(tables.get(table1).isPresent());
        assertTrue(tables.containsKey(table2));
        assertEquals("Exclude materialized view log table", tables.get(table2).get());
        assertTrue(tables.containsKey(table3));
        assertEquals(
                "Exclude table: table name exceeds maximum supported length of 30 characters",
                tables.get(table3).get());
        assertTrue(tables.containsKey(table4));
    }

    @Test
    public void selectChangedRowsQuery_twoTablesWithPkeys() {
        TableRef first = new TableRef("S", "T"), second = new TableRef("S", "T2");
        OracleColumn oracleColumn = new OracleColumn("PKEY", OracleType.create("CHAR"), true, first, Optional.empty());
        ImmutableList<OracleColumn> columnList = ImmutableList.of(oracleColumn);
        Map<TableRef, List<OracleColumn>> columnMap = ImmutableMap.of(first, columnList, second, columnList);

        String expected =
                "SELECT SCN,STATUS,OPERATION_CODE,SEG_OWNER,TABLE_NAME,XID,ROW_ID,ROLLBACK,INFO,CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T\".\"PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END WHEN SEG_OWNER='S' AND TABLE_NAME='T2' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T2\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T2\".\"PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T2\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T2\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"c0\",CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE = 3 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END WHEN SEG_OWNER='S' AND TABLE_NAME='T2' THEN CASE WHEN OPERATION_CODE = 3 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T2\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T2\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"opk0\",CASE WHEN OPERATION_CODE=5 THEN SQL_REDO ELSE '' END \"DDL_EVENT\" FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER IN ('S') AND TABLE_NAME IN ('T2','T')  AND OPERATION_CODE in (1,2,3,5)";

        // TODO, this method is not avilable in any of the classes
//        assertEquals(expected, api.selectChangedRowsQuery(columnMap));
    }

    @Test
    public void selectChangedRowsQuery_twoTablesNoPkeys() {
        TableRef first = new TableRef("S", "T"), second = new TableRef("S", "T2");
        OracleColumn oracleColumn =
                new OracleColumn("NON_PKEY", OracleType.create("CHAR"), false, first, Optional.empty());
        ImmutableList<OracleColumn> columnList = ImmutableList.of(oracleColumn);
        Map<TableRef, List<OracleColumn>> columnMap = ImmutableMap.of(first, columnList, second, columnList);

        String expected =
                "SELECT SCN,STATUS,OPERATION_CODE,SEG_OWNER,TABLE_NAME,XID,ROW_ID,ROLLBACK,INFO,CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')) ELSE 'skip-9Qq2Fa' END WHEN SEG_OWNER='S' AND TABLE_NAME='T2' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T2\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T2\".\"NON_PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T2\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T2\".\"NON_PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"c0\",CASE WHEN OPERATION_CODE=5 THEN SQL_REDO ELSE '' END \"DDL_EVENT\" FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER IN ('S') AND TABLE_NAME IN ('T2','T')  AND OPERATION_CODE in (1,2,3,5)";

        // TODO, this method is not avilable in any of the classes
//        assertEquals(expected, api.selectChangedRowsQuery(columnMap));
    }

    @Test
    public void selectChangedRowsQuery_singleTableNoPkey() {
        TableRef first = new TableRef("S", "T");
        OracleColumn oracleColumn =
                new OracleColumn("NON_PKEY", OracleType.create("CHAR"), false, first, Optional.empty());
        ImmutableList<OracleColumn> columnList = ImmutableList.of(oracleColumn);
        Map<TableRef, List<OracleColumn>> columnMap = ImmutableMap.of(first, columnList);

        String expected =
                "SELECT SCN,STATUS,OPERATION_CODE,SEG_OWNER,TABLE_NAME,XID,ROW_ID,ROLLBACK,INFO,CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"NON_PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"c0\",CASE WHEN OPERATION_CODE=5 THEN SQL_REDO ELSE '' END \"DDL_EVENT\" FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER IN ('S') AND TABLE_NAME IN ('T')  AND OPERATION_CODE in (1,2,3,5)";

        // TODO, this method is not avilable in any of the classes
//        assertEquals(expected, api.selectChangedRowsQuery(columnMap));
    }

    @Test
    public void selectChangedRowsQuery_singleTableWithPkey() {
        TableRef first = new TableRef("S", "T");
        OracleColumn oracleColumn = new OracleColumn("PKEY", OracleType.create("CHAR"), true, first, Optional.empty());
        ImmutableList<OracleColumn> columnList = ImmutableList.of(oracleColumn);
        Map<TableRef, List<OracleColumn>> columnMap = ImmutableMap.of(first, columnList);

        String expected =
                "SELECT SCN,STATUS,OPERATION_CODE,SEG_OWNER,TABLE_NAME,XID,ROW_ID,ROLLBACK,INFO,CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE IN (1,3) AND DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(REDO_VALUE,'\"S\".\"T\".\"PKEY\"')) WHEN OPERATION_CODE = 2 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"c0\",CASE WHEN SEG_OWNER='S' AND TABLE_NAME='T' THEN CASE WHEN OPERATION_CODE = 3 AND DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')=1 THEN (DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE,'\"S\".\"T\".\"PKEY\"')) ELSE 'skip-9Qq2Fa' END  ELSE '' END \"opk0\",CASE WHEN OPERATION_CODE=5 THEN SQL_REDO ELSE '' END \"DDL_EVENT\" FROM SYS.V_$LOGMNR_CONTENTS WHERE SEG_OWNER IN ('S') AND TABLE_NAME IN ('T')  AND OPERATION_CODE in (1,2,3,5)";

        // TODO, this method is not avilable in any of the classes
//        assertEquals(expected, api.selectChangedRowsQuery(columnMap));
    }

    /**
     * TODO - method tableSelectorString is not available in any of the classes
     */
    @Test
    public void tableSelectorString() {
        final int MAX_SCHEMANS_COUNT = 2;
        final int MAX_TABLES = 4;
        Map<String, List<String>> schemas;
        String sql;
        String expected;

        schemas = createTestSet(1, MAX_TABLES - 1);
//        sql = OracleApi.tableSelectorString(schemas, MAX_TABLES);
        expected = " (SEG_OWNER = 'S0' AND (TABLE_NAME IN ('T0','T1','T2'))) ";
//        assertEquals(expected, sql);

        schemas = createTestSet(MAX_SCHEMANS_COUNT, MAX_TABLES);
//        sql = OracleApi.tableSelectorString(schemas, MAX_TABLES);
        expected =
                " (SEG_OWNER = 'S0' AND (TABLE_NAME IN ('T0','T1','T2','T3')) OR SEG_OWNER = 'S1' AND (TABLE_NAME IN ('T0','T1','T2','T3'))) ";
//        assertEquals(expected, sql);

        schemas = createTestSet(MAX_SCHEMANS_COUNT, MAX_TABLES + 1);
//        sql = OracleApi.tableSelectorString(schemas, MAX_TABLES);
        expected =
                " (SEG_OWNER = 'S0' AND (TABLE_NAME IN ('T0','T1','T2','T3') OR TABLE_NAME IN ('T4')) OR SEG_OWNER = 'S1' AND (TABLE_NAME IN ('T0','T1','T2','T3') OR TABLE_NAME IN ('T4'))) ";
//        assertEquals(expected, sql);
    }

    /**
     * isScnOlderThanUpperBoundInterval method is not available in any of the classes
     */
    @Test
    public void test_scnOlderThanUpperBoundIntervalTrue() {

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());
//        assertTrue(api.isScnOlderThanUpperBoundInterval(1L));
    }

    /**
     * isScnOlderThanUpperBoundInterval method is not available in any of the classes
     */
    @Test
    public void test_scnOlderThanUpperBoundIntervalFalse() {

        doReturn(Instant.now()).when(api).convertScnToTimestamp(anyLong());
//        assertFalse(api.isScnOlderThanUpperBoundInterval(1L));
    }

    @Test
    public void test_alterSessionShouldNotExecuteForContainarizedDBWithoutFlag() throws Exception {
        // prepare
        ResultSet mockResultSet = mock(ResultSet.class);
        Connection mockConnection = getMockConnectionForAlterSessionTests(mockResultSet, "YES");
        doReturn(mockConnection).when(dataSource).getConnection();

//        api.connectionManagerWithRetry = spy(new ConnectionManagerWithRetry(dataSource, Optional.of("PDB")));

        // execute
//        api.connectionManagerWithRetry.retry(
//                "testAlterSession",
//                __ -> {
//                    return null;
//                });

        // assert
//        verify(api.connectionManagerWithRetry, times(0)).alterConnectionSession(any(), anyBoolean());
    }
}