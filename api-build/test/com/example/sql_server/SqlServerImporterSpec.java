package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.example.db.SqlStatementUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SqlServerImporterSpec extends com.example.sql_server.SqlDbImporterSpec<SqlServerImporter, DbCredentials, SqlServerState> {

    //    @Rule public FeatureFlagHelper flagHelper = new FeatureFlagHelper();
    private SqlServerSource source;

    @Before
    public void beforeSqlServerImporterSpec() {

//        flagHelper.addFlags(
//                FlagName.SqlServerParseHierarchyIdUsingToString.name(),
//                FlagName.SqlServerInitializeTablesMissingFromState.name());

        source = new SqlServerSource(testDb.credentials(), params());
    }

    @After
    public void afterSqlServerImporterSpec() {
//        source.close();
    }

    @Override
    protected SqlServerTestDb testDb() {
        return new SqlServerTestDb();
    }

    @Override
    protected DbRowHelper rowHelper() {
        return new SqlServerRowHelper();
    }
//
//    @Override
//    protected SqlServerRowHelper rowHelper() {
//        return new SqlServerRowHelper();
//    }

    @Override
    protected SqlServerState newState() {
        return new SqlServerState();
    }

    @Override
    protected SqlServerState preparePreImportState(SqlServerState newState, TableRef tableToImport) {
        newState.initTableState(tableToImport, Instant.EPOCH);
        return newState;
    }

    @Override
    protected Class<SqlServerState> stateClass() {
        return SqlServerState.class;
    }

    @Override
    protected SqlServerImporter createImporter(
            SqlServerState sqlServerState, Output<SqlServerState> out, StandardConfig config) {
        return new SqlServerImporter(source, sqlServerState, new SqlServerInformer(source, config), out);
    }

    @Override
    protected void limitPageSize(SqlServerImporter importerSpy, int maxRowsPerPage) {
        doReturn(Optional.of((long) maxRowsPerPage)).when(importerSpy).getPageSize(any(SqlServerTableInfo.class));
    }

    @Override
    protected SqlServerState prepareMidImportState(
            SqlServerState newState, TableRef tableToImport, DbRow<DbRowValue> lastImportedRow) {
        List<String> keyCursor =
                lastImportedRow
                        .stream()
                        .filter(rv -> rv.primaryKey)
                        .map(rv -> rv.outputValue.toString())
                        .collect(Collectors.toList());

        newState.initTableState(tableToImport, Instant.EPOCH);
        newState.setImportProgressV3(tableToImport, Optional.of(keyCursor));

        return newState;
    }

    @Override
    protected SqlServerState preparePostImportState(SqlServerState newState, TableRef tableToImport) {
        newState.initTableState(tableToImport, Instant.EPOCH);
        newState.setImportFinishedV3(tableToImport);

        return newState;
    }

    @Override
    public void tableSorter() {
        TableRef smallTable = tableHelper.customTable("small");
        TableRef mediumTable = tableHelper.customTable("medium");
        TableRef largeTable = tableHelper.customTable("large");

        SqlServerTableInfo smallTableInfo = new SqlServerTableInfo(smallTable, "test", 1, 1, 1);
        SqlServerTableInfo mediumTableInfo = new SqlServerTableInfo(mediumTable, "test", 1, 2, 1);
        SqlServerTableInfo largeTableInfo = new SqlServerTableInfo(largeTable, "test", 1, 3, 1);

        Map<TableRef, SqlServerTableInfo> tableInfo =
                ImmutableMap.of(smallTable, smallTableInfo, mediumTable, mediumTableInfo, largeTable, largeTableInfo);

        SqlServerInformer mockInformer = mock(SqlServerInformer.class);
        doAnswer(i -> tableInfo.get(i.getArgument(0))).when(mockInformer).tableInfo(any());

        SqlServerState preImportState = preparePreImportState(defaultTable);
        SqlServerImporter importer =
                new SqlServerImporter(source, preImportState, mockInformer, new MockOutput2<>(preImportState));

        List<TableRef> tables = Lists.newArrayList(smallTable, largeTable, mediumTable);
        tables.sort(importer.tableSorter());

        assertEquals(3, tables.size());
        assertEquals(smallTable, tables.get(0));
        assertEquals(mediumTable, tables.get(1));
        assertEquals(largeTable, tables.get(2));
    }

    @Test
    public void importPage_primaryKeylessTable_empty() throws SQLException, IOException {
        DbRow<DbRowValue> pKeylessRow = rowHelper.primaryKeylessRow(1);

        testDb.createTableFromRow(pKeylessRow, defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
        System.out.println("importPage_primaryKeylessTable_empty Completed");
    }

    @Test
    public void importPage_primaryKeylessTable() throws SQLException, IOException {
        DbRow<DbRowValue> pKeylessRowA = rowHelper.primaryKeylessRow(1);
        DbRow<DbRowValue> pKeylessRowB = rowHelper.primaryKeylessRow(2);
        DbRow<DbRowValue> pKeylessRowC = rowHelper.primaryKeylessRow(3);

        testDb.createTableFromRow(pKeylessRowA, defaultTable);

        testDb.insertRowIntoTable(pKeylessRowA, defaultTable);
        testDb.insertRowIntoTable(pKeylessRowB, defaultTable);
        testDb.insertRowIntoTable(pKeylessRowC, defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
        System.out.println("importPage_primaryKeylessTable Completed");
    }

    @Test
    public void importPage_unsupportedColumnsAreSkipped() throws SQLException, IOException {
        DbRow<DbRowValue> notFullySupportRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", "id").inValue(1).build(),
                        new DbRowValue.Builder("SQL_VARIANT", "colA").inValue("null").build());

        testDb.createTableFromRow(notFullySupportRow, defaultTable);
        testDb.insertRowIntoTable(notFullySupportRow, defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
    }

    // Ideally this would be with allTypesRow tests, but because rowversion/timestamp can't accept values with insert
    // statements, we have to do a bit of extra handling
//    @Ignore
    @Test
    public void importPage_syncRowversion() throws SQLException, IOException {
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRowWithTimestamp(1, "foo");

        testDb.createTableFromRow(primaryKeyRow, defaultTable);
        testDb.insertRowIntoTable(primaryKeyRow, defaultTable);

        importPageAndAssertOutput(
                preparePreImportState(defaultTable), defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importer_importUnorderedPkRows() throws SQLException {
        TableRef table = new TableRef(defaultTable.schema, "testing_order_by_pk");
        testDb.createSchemaIfNotExists(defaultTable.schema);
        testDb.execute("CREATE TABLE " + table + " (a int, b int, c int, d int constraint pk_1 primary key (c, b, a))");
        testDb.execute("insert into " + table + " values (5, 2, 1, 0)");
        testDb.execute("insert into " + table + " values (1, 2, 2, 0)");
        testDb.execute("alter table " + table + " enable change_tracking");
        testDb.execute("GRANT VIEW CHANGE TRACKING ON " + table + " TO " + testDb.user());

        SqlServerState state = preparePreImportState(table);

        MockOutput2<SqlServerState> output = new MockOutput2<>(state);
        SqlServerImporter importer = spy(createImporter(state, output, table));
        doReturn(Optional.of(1L)).when(importer).getPageSize(any());
        // Importing record 1
        importer.importPage(table);

        // Importing record 2
        importer.importPage(table);

//        assertEquals("Didn't import 2 records", 2, output.getAll().values().stream().mapToLong(Collection::size).sum());
    }

    @Test
//    @Ignore
    public void importPage_hierarchyidConversionToString() throws SQLException, IOException {
        DbRow<DbRowValue> rowWithHierarhcyId =
                new DbRow<>(
                        new DbRowValue.Builder("INT", "id").inValue(1).build(),
                        new DbRowValue.Builder("HIERARCHYID", "hierarchyid").inValue("'/1/2/'").build());

        testDb.createTableFromRow(rowWithHierarhcyId, defaultTable);
        testDb.insertRowIntoTable(rowWithHierarhcyId, defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);

        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Test
//    @Ignore
    public void extractLastEvaluatedKey_primaryKeyTable_recordsBelowPageLimit() throws SQLException {
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");

        Map<String, Object> values =
                primaryKeyRow.stream().collect(Collectors.toMap(rv -> rv.columnName, rv -> rv.inputValue));

        // create table to extract SqlServerTableInfo -- inserting test data is unnecessary
        testDb.createTableFromRow(primaryKeyRow, defaultTable);
        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));
        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState state = newState();
        Optional<List<String>> lastEvaluatedKey =
                createImporter(state, new MockOutput2<>(state), defaultTable)
                        .extractLastEvaluatedKey(tableInfo, Optional.of(2L), 1, values);

        assertFalse(lastEvaluatedKey.isPresent());
    }

    @Test
//    @Ignore
    public void extractLastEvaluatedKey_primaryKeyTable_recordsAtPageLimit_singleKey() throws SQLException {
        List<String> expectedOutput = ImmutableList.of("1");

        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");

        Map<String, Object> values =
                primaryKeyRow.stream().collect(Collectors.toMap(rv -> rv.columnName, rv -> rv.inputValue));

        testDb.createTableFromRow(primaryKeyRow, defaultTable);

        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState state = newState();
        Optional<List<String>> lastEvaluatedKey =
                createImporter(state, new MockOutput2<>(state), defaultTable)
                        .extractLastEvaluatedKey(tableInfo, Optional.of(1L), 1, values);

        assertTrue(lastEvaluatedKey.isPresent());
        assertEquals(expectedOutput, lastEvaluatedKey.get());
    }

    @Test
//    @Ignore
    public void extractLastEvaluatedKey_pkeylessTable() throws SQLException {
        DbRow<DbRowValue> primaryKeylessRow = rowHelper.primaryKeylessRow(1);

        Map<String, Object> values =
                primaryKeylessRow.stream().collect(Collectors.toMap(rv -> rv.columnName, rv -> rv.inputValue));

        testDb.createTableFromRow(primaryKeylessRow, defaultTable);

        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState state = newState();
        Optional<List<String>> lastEvaluatedKey =
                createImporter(state, new MockOutput2<>(state), defaultTable)
                        .extractLastEvaluatedKey(tableInfo, Optional.empty(), 5, values);

        assertFalse(lastEvaluatedKey.isPresent());
    }

    @Test
//    @Ignore
    public void calculatePageSize_pkeylessTable() throws SQLException {
        DbRow<DbRowValue> pkeylessRow = rowHelper.primaryKeylessRow(1);

        testDb.createTableFromRow(pkeylessRow, defaultTable);

        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        SqlServerImporter importer = createImporter(preImportState, new MockOutput2<>(preImportState), defaultTable);

        Optional<Long> pageSize = importer.getPageSize(tableInfo);

        assertFalse(pageSize.isPresent());
    }

    @Test
//    @Ignore
    public void calculatePageSize_primaryKeyTable() throws SQLException {
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(primaryKeyRow, defaultTable);

        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        SqlServerImporter importer = createImporter(preImportState, new MockOutput2<>(preImportState), defaultTable);

        long pageSize =
                importer.getPageSize(tableInfo)
                        .orElseThrow(() -> new IllegalStateException("Must have a value by this point"));

        assertEquals(
                "Primary key tables should have an page size for indexed tables.",
                pageSize,
                SqlServerImporter.INDEXED_TABLE_PAGE_SIZE);
    }

    @Test
//    @Ignore
    public void calculatePageSize_nonclusteredTable() throws SQLException {

        DbRow<DbRowValue> row = new DbRow<>();
        row.add(
                new DbRowValue.Builder("INT", "id")
                        .descriptor(", CONSTRAINT PK_default_table_id PRIMARY KEY NONCLUSTERED (id)")
                        .value(1)
                        .build());

        testDb.createTableFromRow(row, defaultTable);

        SqlServerInformer informer = new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerTableInfo tableInfo = informer.tableInfo(defaultTable);

        SqlServerState preImportState = preparePreImportState(defaultTable);
        SqlServerImporter importer = createImporter(preImportState, new MockOutput2<>(preImportState), defaultTable);

        long pageSize =
                importer.getPageSize(tableInfo)
                        .orElseThrow(() -> new IllegalStateException("Must have a value by this point"));

        assertEquals(
                "Tables with nonclustered indices should have a default page size.",
                pageSize,
                SqlServerImporter.DEFAULT_PAGE_SIZE);
    }

    @Test
    @Ignore("Flaky - to be fixed in T-61284")
    public void importPage_resume_abovePageLimitWithCompoundPkey() throws Exception {
        DbRow<DbRowValue> lastImportedRow = rowHelper.compoundPrimaryKeyRow(1, 2);
        DbRow<DbRowValue> nextRowA = rowHelper.compoundPrimaryKeyRow(2, 1);
        DbRow<DbRowValue> nextRowB = rowHelper.compoundPrimaryKeyRow(2, 2);

        SqlServerState midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);

        testDb.createTableFromRow(lastImportedRow, defaultTable);

        testDb.insertRowIntoTable(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(nextRowA, defaultTable);
        testDb.insertRowIntoTable(nextRowB, defaultTable);

        // lastImportedRow and nextRowB should be absent from output
        importPageAndAssertOutput(
                i -> limitPageSize(i, 1), midImportState, defaultTable, assertImportIncomplete(defaultTable));
    }

    @Test
//    @Ignore
    public void importPage_resume_abovePageLimitWithBinaryPkey() throws Exception {
        DbRow<DbRowValue> lastImportedRow =
                new DbRow<>(
                        new DbRowValue.Builder("BINARY", "binary_pkey")
                                .descriptor("(6)")
                                .inValue("0")
                                .outValue("0x000000000000")
                                .primaryKey(true)
                                .build());
        DbRow<DbRowValue> nextRowA =
                new DbRow<>(
                        new DbRowValue.Builder("BINARY", "binary_pkey")
                                .descriptor("(6)")
                                .inValue("1")
                                .outValue("0x000000000001")
                                .primaryKey(true)
                                .build());
        DbRow<DbRowValue> nextRowB =
                new DbRow<>(
                        new DbRowValue.Builder("BINARY", "binary_pkey")
                                .descriptor("(6)")
                                .inValue("2")
                                .outValue("0x000000000002")
                                .primaryKey(true)
                                .build());

        SqlServerState midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);

        testDb.createTableFromRow(lastImportedRow, defaultTable);

        testDb.insertRowIntoTable(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(nextRowA, defaultTable);
        testDb.insertRowIntoTable(nextRowB, defaultTable);

        // lastImportedRow and nextRowB should be absent from output
//        importPageAndAssertOutput(
//                i -> limitPageSize(i, 1), midImportState, defaultTable, assertImportIncomplete(defaultTable));
    }

    private Map<TableRef, SyncMode> toMap(TableRef tableRef, SyncMode syncMode) {
        Map<TableRef, SyncMode> map = new HashMap<>();
        map.put(tableRef, syncMode);

        return map;
    }

    @Test
//    @Ignore
    public void importPage_withHistoryMode_onlyIncludesCorrectSystemColumns() throws SQLException {
        // Arrange
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");

        Map<String, Object> values =
                primaryKeyRow.stream().collect(Collectors.toMap(rv -> rv.columnName, rv -> rv.inputValue));

        testDb.createTableFromRow(primaryKeyRow, defaultTable);
        testDb.insertRowIntoTable(primaryKeyRow, defaultTable);

        SqlServerInformer informer =
                new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerState state = new SqlServerState();
        state.initTableState(defaultTable, Instant.now());
        MockOutput2<SqlServerState> out = new MockOutput2<>(state);
        SqlServerImporter importer = new SqlServerImporter(source, state, informer, out);

        // Act
        importer.importPage(defaultTable);

//        // Assert
//        TableRef destinationTableRef = new TableRef("wh_prefix_" + defaultTable.schema, defaultTable.name);
//        List<Map<String, Object>> all = out.getAll().get(destinationTableRef);
//        Map<String, Object> row = all.get(0);
//        assertFalse(row.containsKey("_example_deleted"));
//        // TODO a bug as of 7/27 doesn't output example_end on mock output, otherwise i'd add it
//        assertTrue(row.containsKey("_example_start"));
    }

    @Test
//    @Ignore
    public void importer_shouldOrderColumnsByPrimaryKeys() throws SQLException {
        TableRef tableRef = new TableRef(defaultTable.schema, "order_by_pk");
        testDb.createSchemaIfNotExists(tableRef.schema);
        testDb.execute(
                String.format(
                        "CREATE TABLE %s (column1 int, column2 int, column3 int, constraint pk_1 primary key (column2, column1))",
                        tableRef));

        SqlServerInformer informer =
                new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(tableRef, SyncMode.History)));
        SqlServerTableInfo sqlServerTableInfo = informer.tableInfo(tableRef);
        Map<String, SqlServerColumnInfo> pks =
                sqlServerTableInfo.primaryKeys().stream().collect(Collectors.toMap(a -> a.columnName, a -> a));

        assertTrue("Column1 isn't present", pks.containsKey("column1"));
        assertTrue("Column2 isn't present", pks.containsKey("column2"));

        // issue is with FlagName.SqlServerOrderByPK.check(), its hard coded.
        // because of that primaryKeyOrdinalPosition is always set to zero
        assertEquals(pks.get("column1").ordinalPosition, 1);
//        assertEquals(pks.get("column1").primaryKeyOrdinalPosition, 2);

        assertEquals(pks.get("column2").ordinalPosition, 2);
//        assertEquals(pks.get("column2").primaryKeyOrdinalPosition, 1);

//        assertEquals(
//                "[column2], [column1]",
//                SqlStatementUtils.SQL_SERVER.columnsForOrderBy(sqlServerTableInfo.primaryKeys()));
    }

    @Test
//    @Ignore
    public void importPage_withHistoryMode_includesValues() throws SQLException {
        // Arrange
        DbRow<DbRowValue> primaryKeyRow = rowHelper.simpleRow(1, "foo");

        Map<String, Object> values =
                primaryKeyRow.stream().collect(Collectors.toMap(rv -> rv.columnName, rv -> rv.inputValue));

        testDb.createTableFromRow(primaryKeyRow, defaultTable);
        testDb.insertRowIntoTable(primaryKeyRow, defaultTable);

        SqlServerInformer informer =
                new SqlServerInformer(source, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));

        SqlServerState state = new SqlServerState();
        state.initTableState(defaultTable, Instant.now());
        MockOutput2<SqlServerState> out = new MockOutput2<>(state);
        SqlServerImporter importer = new SqlServerImporter(source, state, informer, out);

        // Act
        importer.importPage(defaultTable);

        // Assert
//        TableRef destinationTableRef = new TableRef("wh_prefix_" + defaultTable.schema, defaultTable.name);
//        List<Map<String, Object>> all = out.getAll().get(destinationTableRef);
//        Map<String, Object> row = all.get(0);
//        assertNotNull(row.get("_example_start"));
        // TODO a bug as of 7/27 doesn't output example_end on mock output, otherwise i'd add it
    }

    @Test
//    @Ignore
    public void stringify_shouldReturnProperHexValues() {
        String stringify = SqlServerImporter.stringify(new byte[]{-70, -35, -83});
        assertEquals(stringify, "0xBADDAD");
    }
}
