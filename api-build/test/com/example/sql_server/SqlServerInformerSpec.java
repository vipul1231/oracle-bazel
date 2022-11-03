package com.example.sql_server;

import com.example.core.*;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.google.common.collect.Ordering;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;
import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;
import static com.example.sql_server.SqlServerInformer.isCDCEnabled;
import static org.junit.Assert.*;

public class SqlServerInformerSpec extends DbTest<DbCredentials, SqlServerState> {

    private SqlServerSource source;

    @Before
    public void beforeSqlServerInformerSpec() {
        source = new SqlServerSource(testDb.credentials(), params());
    }

    @After
    public void afterSqlServerInformerSpec() {
        source.close();
    }

    @Override
    protected SqlServerTestDb testDb() {
        return new SqlServerTestDb();
    }

    @Override
    protected SqlDbTableHelper tableHelper() {
        return new SqlDbTableHelper();
    }

    @Override
    protected SqlServerRowHelper rowHelper() {
        return new SqlServerRowHelper();
    }

    @Override
    protected SqlServerState newState() {
        return new SqlServerState();
    }

    @Override
    protected Class<SqlServerState> stateClass() {
        return SqlServerState.class;
    }

    private SqlServerInformer createInformer() {
        SqlServerService service = new SqlServerService();
        return service.informer(source, new StandardConfig());
    }

    @Test
    public void informer_pKeyTableIsDetectedEx() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.simpleRow(1, "test_value");

        testDb.createTableFromRow(simpleRow, defaultTable);
        testDb.insertRowIntoTable(simpleRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertEquals(
                "Change data type was not detected",
                tableInfoMap.get(defaultTable).changeType,
                SqlServerChangeType.CHANGE_TRACKING);
        assertTrue("Table should not be excluded", tableInfoMap.get(defaultTable).excludeReasons.isEmpty());
    }

    @Test
//    @Ignore
    public void informer_pKeylessTableIsDetected() throws SQLException {
        DbRow<DbRowValue> pkeylessRow = rowHelper.primaryKeylessRow(1);

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertEquals(
                "Change data type was not detected",
                tableInfoMap.get(defaultTable).changeType,
                SqlServerChangeType.CHANGE_DATA_CAPTURE);
        assertTrue("Table should not be excluded", tableInfoMap.get(defaultTable).excludeReasons.isEmpty());
    }

    @Test
//    @Ignore
    public void informer_columnsOrderedUsingOrdinalPosition() throws SQLException {
        DbRow<DbRowValue> allTypesRow = rowHelper.allTypesRow(1);

        testDb.createTableFromRow(allTypesRow, defaultTable);
        testDb.insertRowIntoTable(allTypesRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));

        SqlServerTableInfo tableInfo = tableInfoMap.get(defaultTable);

        assertTrue(
                "Columns must be sorted by their ordinal positions",
                Ordering.natural()
                        .isOrdered(
                                tableInfo
                                        .sourceColumnInfo()
                                        .stream()
                                        .map(ci -> ci.ordinalPosition)
                                        .collect(Collectors.toList())));
    }

    @Test
    public void informer_tableWithoutSelectPermissionsIsExcluded() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.simpleRow(1, "test_value");

        testDb.createTableFromRow(simpleRow, defaultTable);
        testDb.insertRowIntoTable(simpleRow, defaultTable);

        testDb.execute(String.format("DENY SELECT ON OBJECT::%s TO %s", defaultTable.toLongString(), testDb.credentials().user));

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertTrue(
                "Table without select permissions should be excluded",
                tableInfoMap.get(defaultTable).excludeReasons.contains(ExcludeReason.ERROR_MISSING_SELECT));
    }

    @Test
//    @Ignore
    public void informer_tableWithoutAnyTrackingIsExcluded() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.simpleRow(1, "test_value");

        testDb.createTableFromRow(simpleRow, defaultTable);
        testDb.execute("ALTER TABLE " + defaultTable.toLongString() + " DISABLE CHANGE_TRACKING");

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertTrue(
                "Table without any tracking enabled must be excluded",
                tableInfoMap.get(defaultTable).excludeReasons.contains(ExcludeReason.ERROR_CDC_AND_CT_NOT_ENABLED));
    }

    @Test
    public void informer_tableWithoutViewCTPermissionIsExcluded() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.simpleRow(1, "test_value");

        testDb.createTableFromRow(simpleRow, defaultTable);
        testDb.insertRowIntoTable(simpleRow, defaultTable);

        testDb.execute(
                String.format(
                        "DENY VIEW CHANGE TRACKING ON OBJECT::%s TO %s", defaultTable.toLongString(), testDb.credentials().user));

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertTrue(
                "Table without VIEW CHANGE TRACKING permissions should be excluded",
                tableInfoMap.get(defaultTable).excludeReasons.contains(ExcludeReason.ERROR_CT_VIEW_CHANGE_TRACKING));
    }

    @Test
//    @Ignore
    public void informer_updateMinValidChangeVersion() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.simpleRow(1, "test_value");
        testDb.createTableFromRow(simpleRow, defaultTable);

        SqlServerInformer informer = createInformer();
        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);
        long minVersionBeforeTruncation = tableInfo.minChangeVersion;

        // Truncate the table in order to force the min valid change version to increase
        testDb.insertRowIntoTable(simpleRow, defaultTable);
        testDb.execute("TRUNCATE TABLE " + defaultTable.toLongString());

        informer.updateMinChangeVersions();
        long minVersionAfterTruncation = tableInfo.minChangeVersion;

        assertTrue(
                "The minimum valid change version must be higher after a table is truncated",
                minVersionBeforeTruncation < minVersionAfterTruncation);
    }

    @Test
    public void informer_tableWithoutSelectOnAllPkeysIsExcluded() throws SQLException {
        DbRow<DbRowValue> simpleRow = rowHelper.compoundPrimaryKeyRow(1, 2);

        testDb.createTableFromRow(simpleRow, defaultTable);
        testDb.insertRowIntoTable(simpleRow, defaultTable);

        testDb.execute(
                String.format(
                        "DENY SELECT ON %s (%s) TO %s",
                        testDb.quote(defaultTable), PRIMARY_KEY_NAME + "_B", testDb.credentials().user));

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertTrue(
                "Table without full select permissions on primary key columns should be excluded",
                tableInfoMap.get(defaultTable).excludeReasons.contains(ExcludeReason.ERROR_CT_SELECT_ON_PK));
    }

    @Test
    @Ignore
    public void informer_getTableCDCColumns_CDCIsDisabledOnDB() throws SQLException {
        testDb.execute("EXEC sys.sp_cdc_disable_db;");

        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "cdc_disabled");
        testDb.createTableFromRow(row, defaultTable);

        try (Connection c = testDb.dataSource().getConnection()) {
            assertThrows(
                    SqlServerInformer.CDCNotEnabledForTable.class,
                    () -> SqlServerInformer.getTableCDCColumns(c, defaultTable));
        }
    }

    @Test
    @Ignore
    public void informer_getTableCDCColumns_CDCIsDisabledOnTable() throws SQLException, InterruptedException {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "cdc_disabled");
        testDb.createTableFromRow(row, defaultTable);

        try (Connection c = testDb.dataSource().getConnection()) {
            assertThrows(
                    SqlServerInformer.CDCNotEnabledForTable.class,
                    () -> SqlServerInformer.getTableCDCColumns(c, defaultTable));
        }
    }

    @Test
    @Ignore
    public void informer_pkeylessTableWithMultipleCDCInstancesIsExcluded() throws SQLException {
        DbRow<DbRowValue> pkeylessRow = rowHelper.primaryKeylessRow(1);

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @capture_instance = N'extra_instance'",
                        defaultTable.schema, defaultTable.name));
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertTrue(
                "Table with multiple CDC instances should be excluded",
                tableInfoMap.get(defaultTable).excludeReasons.contains(ExcludeReason.ERROR_CDC_TOO_MANY_INSTANCES));
    }

    @Test
    @Ignore
    public void informer_pkeylessTableWithoutFullCDC_TableIsIncluded() throws SQLException {
        DbRow<DbRowValue> pkeylessRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", UPDATABLE_COLUMN_NAME).value(3).build(),
                        new DbRowValue.Builder("INT", "untracked_column").value(5).build());

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        ((SqlServerTestDb) testDb).disableCDC(defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @captured_column_list = N'%s'",
                        defaultTable.schema, defaultTable.name, UPDATABLE_COLUMN_NAME));
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertEquals(
                "Change data type was not detected",
                tableInfoMap.get(defaultTable).changeType,
                SqlServerChangeType.CHANGE_DATA_CAPTURE);
        assertTrue("Table with full CDC will be present", tableInfoMap.get(defaultTable).excludeReasons.isEmpty());
        assertFalse("Warning should is generated", informer.dashboardWarnings().isEmpty());
    }

    @Test
    @Ignore
    public void cdc_excludeTableWithUntrackedPkey() throws SQLException {
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");
        primaryKeyRow.add(new DbRowValue.Builder("INT", "NEW_COLUMN").inValue(1).descriptor("UNIQUE NOT NULL").build());
        ((SqlServerTestDb) testDb).createTableFromRow(primaryKeyRow, defaultTable, false);

        testDb.execute(
                String.format(
                        "CREATE UNIQUE INDEX non_pkey_index ON [%s].[%s] (%s)",
                        defaultTable.schema, defaultTable.name, "NEW_COLUMN"));

        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_disable_table @source_schema = N'%s', @source_name = N'%s', @capture_instance = 'all'",
                        defaultTable.schema, defaultTable.name));

        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @captured_column_list = N'%s', @index_name = N'%s'",
                        defaultTable.schema, defaultTable.name, "NEW_COLUMN", "non_pkey_index"));

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
//        assertTrue(
//                "Primary keys must be tracked by CDC",
//                tableInfoMap.get(defaultTable).excludeReasons.contains(ERROR_CDC_PKEY_MUST_BE_TRACKED));
    }

    @Test
//    @Ignore
    public void informer_pkeylessTableWithoutFullCDC_ColumnsAreExcluded() throws SQLException {
        String newCdcExcludedColumn = "untracked_column";

        DbRow<DbRowValue> pkeylessRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", UPDATABLE_COLUMN_NAME).value(3).build(),
                        new DbRowValue.Builder("INT", newCdcExcludedColumn).value(5).build());

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        ((SqlServerTestDb) testDb).disableCDC(defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @captured_column_list = N'%s'",
                        defaultTable.schema, defaultTable.name, UPDATABLE_COLUMN_NAME));
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerInformer informer = createInformer();
        Map<TableRef, SqlServerTableInfo> tableInfoMap = informer.latestTableInfo();

        assertTrue("Table was not found from the informer", tableInfoMap.containsKey(defaultTable));
        assertEquals(
                "Change data type was not detected",
                tableInfoMap.get(defaultTable).changeType,
                SqlServerChangeType.CHANGE_DATA_CAPTURE);
        assertTrue("Table with full CDC will be present", tableInfoMap.get(defaultTable).excludeReasons.isEmpty());
//        assertFalse(
//                "Newly created column shouldn't be created in destination",
//                tableInfoMap.get(defaultTable).tableDefinition().types.containsKey(newCdcExcludedColumn));
    }

    @Test
//    @Ignore
    public void getRawColumnInfoMap_queriesAllWithoutFlag() throws SQLException {
        DbRow<DbRowValue> row = new DbRow<>(new DbRowValue.Builder("INT", UPDATABLE_COLUMN_NAME).value(1).build());
        testDb.createTableFromRow(row, defaultTable);
        TableRef newTable = new TableRef(defaultTable.schema, "new_table");
        testDb.createTableFromRow(row, newTable);

        StandardConfig standardConfig = new StandardConfig();
        standardConfig.setExcludeByDefault(true);
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.putTable(defaultTable.name, new TableConfig());
        standardConfig.putSchema(defaultTable.schema, schemaConfig);

        Map<TableRef, SqlServerTableInfo> tableInfo;

        SqlServerInformer sqlServerInformer = new SqlServerInformer(source, standardConfig);

        tableInfo = sqlServerInformer.latestTableInfo();

        assertEquals(tableInfo.get(defaultTable).sourceColumns().size(), 1);
        assertEquals(tableInfo.get(newTable).sourceColumns().size(), 1);
    }

    @Test
//    @Ignore
    public void informer_haveTablesAtSource_True() throws SQLException {
        DbRow<DbRowValue> sqlDbRowValues = rowHelper.simpleRow(1, "1");
        testDb.createTableFromRow(sqlDbRowValues, defaultTable);

        SqlServerInformer informer = createInformer();

        assertTrue(informer.haveTablesAtSource());
    }

    @Test
//    @Ignore
    public void displayAllColumnsInTableUsingCT() throws SQLException {
        DbRow<DbRowValue> dbRowValues = rowHelper.simpleRow(1, "1");
        testDb.createTableFromRow(dbRowValues, defaultTable);
        testDb.execute("ALTER TABLE " + defaultTable.toLongString() + " ADD another_column varchar(20)");

        SqlServerInformer informer = createInformer();

        SqlServerTableInfo tableInfo = informer.latestTableInfo().get(defaultTable);
        // rowHelper.simpleRow(1, "1") - There is an issue in rowHelper
        // TODO recheck  with new RowHelper
        assertEquals(tableInfo.includedColumnInfo().size(), 2);
        Set<String> columnNames =
                tableInfo.includedColumnInfo().stream().map(ci -> ci.columnName).collect(Collectors.toSet());
//        assertTrue(columnNames.contains("UPDATABLE_COLUMN"));
//        assertTrue(columnNames.contains("PRIMARY_KEY"));
        assertTrue(columnNames.contains("another_column"));
    }

    @Test
    public void test_isCDCEnabled() throws SQLException {
        // TestDb starts off with CDC as enabled
        try (Connection c = testDb.dataSource().getConnection()) {
            assertTrue(isCDCEnabled(c));
        }

        // Disable CDC and check if method returns false
        testDb.execute("EXEC sys.sp_cdc_disable_db");
        try (Connection c = testDb.dataSource().getConnection()) {
            assertFalse(isCDCEnabled(c));
        }

        // Calling the informer should not error, even if CDC is disabled
        createInformer().latestTableInfo();
    }
}
