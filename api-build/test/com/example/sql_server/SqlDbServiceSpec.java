package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.example.core2.Output;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.example.db.DbService;
import com.google.common.collect.ImmutableSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.util.Set;

import static com.example.db.DbRowValue.*;
import static org.mockito.Mockito.mock;

public abstract class SqlDbServiceSpec<
        Creds extends DbCredentials, State, Service extends DbService<Creds, State, ?, ?, ?>> extends DbTest{

    @Rule public FeatureFlagHelper flagHelper = new FeatureFlagHelper();

//    @Override
    protected SqlDbTableHelper tableHelper() {
        return new SqlDbTableHelper();
    }

    /**
     * Created to avoid adding more parameters for the Source, Informer, and Importer. Responsible for creating a
     * SqlDbSource, SqlDbInformer, and DbImporter, then calling SqlDbService#prepareSync.
     */
    protected abstract void executePrepareSync(
            Output<State> output,
            Service service,
            State state,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables);

    protected abstract State midImportState(Set<TableRef> importStarted, Set<TableRef> importNotStarted);

    @Test
    @Ignore
    public void sync_initialImport_primaryKeylessAllTypes() throws Exception {
        DbRow<DbRowValue> allTypesRow = rowHelper.primaryKeylessAllTypesRow(1);

        testDb.createTableFromRow(allTypesRow, defaultTable);
        testDb.insertRowIntoTable(allTypesRow, defaultTable);

//        updateAndAssertOutput(doNothingOnIncrementalUpdate(), newState(), defaultTable);
    }

    @Test
    @Ignore
    public void sync_incrementalUpdates_primaryKeylessAllTypes() throws Exception {
        DbRow<DbRowValue> allTypesRow = rowHelper.primaryKeylessAllTypesRow(1);

        testDb.createTableFromRow(allTypesRow, defaultTable);

        // configure state - to avoid DDL change logs, we do this after table creation
//        State state = incrementalUpdateState(defaultTable);
//
//        testDb.insertRowIntoTable(allTypesRow, defaultTable);

//        updateAndAssertOutput(failOnImportTable(), state, defaultTable);
    }

    @Test
    @Ignore
    public void sync_replicateForeignKeys_refPrimaryKeys() throws Exception {
        TableRef parentWithPKey = tableHelper.customTable("parent");
        TableRef childWithPKey = tableHelper.customTable("child");

        DbRow<DbRowValue> parentRow = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> childRow = rowHelper.simpleRow(2, "bar");

        childRow.add(rowHelper.foreignKeyRefRowValue(FOREIGN_KEY_NAME, 1, parentWithPKey, PRIMARY_KEY_NAME));

        testDb.createTableFromRow(parentRow, parentWithPKey);
        testDb.createTableFromRow(childRow, childWithPKey);

        testDb.insertRowIntoTable(parentRow, parentWithPKey);
        testDb.insertRowIntoTable(childRow, childWithPKey);

//        updateAndAssertOutput(newState(), ImmutableList.of(parentWithPKey, childWithPKey));
    }

    @Test
    @Ignore
    public void sync_dontReplicateForeignKeys_refNonPrimaryKey() throws Exception {
        TableRef parentNonPKey = tableHelper.customTable("parent_no_pkey");
        TableRef childNonPKey = tableHelper.customTable("child_no_pkey");

        DbRow<DbRowValue> parentRow = rowHelper.primaryKeylessRow(1);
        DbRow<DbRowValue> childRow = rowHelper.simpleRow(2, "bar");

        childRow.add(rowHelper.foreignKeyRefRowValue(FOREIGN_KEY_NAME, 1, parentNonPKey, UPDATABLE_COLUMN_NAME));

        testDb.createTableFromRow(parentRow, parentNonPKey);
        testDb.createTableFromRow(childRow, childNonPKey);

        testDb.insertRowIntoTable(parentRow, parentNonPKey);
        testDb.insertRowIntoTable(childRow, childNonPKey);

//        updateAndAssertOutput(newState(), ImmutableList.of(parentNonPKey, childNonPKey));
    }

    @Test
    @Ignore
    public void sync_dontReplicateForeignKeys_manyColumnsRefSameTable() throws Exception {
        TableRef parent = tableHelper.customTable("parent");
        TableRef child = tableHelper.customTable("child");

        DbRow<DbRowValue> parentRow = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> childRow = rowHelper.simpleRow(2, "bar");

        childRow.add(rowHelper.foreignKeyRefRowValue("firstRef", 1, parent, PRIMARY_KEY_NAME));
        childRow.add(rowHelper.foreignKeyRefRowValue("secondRef", 1, parent, PRIMARY_KEY_NAME));

        testDb.createTableFromRow(parentRow, parent);
        testDb.createTableFromRow(childRow, child);

        testDb.insertRowIntoTable(parentRow, parent);
        testDb.insertRowIntoTable(childRow, child);

//        updateAndAssertOutput(newState(), ImmutableList.of(parent, child));
    }

    @Test
    @Ignore
    public void sync_initialImport_shouldHandleAnyCase() throws Exception {
        sync_initialImport_shouldHandleAnyCaseTest();
    }

    private void sync_initialImport_shouldHandleAnyCaseTest() throws Exception {
        TableRef lowerCaseTable = tableHelper.customTable("lower_case_schema", "lower_case_table");
        TableRef mixedCaseTable = tableHelper.customTable("MiXeD_CaSe_ScHeMa", "MiXeD_CaSe_TaBlE");
        TableRef upperCaseTable = tableHelper.customTable("UPPER_CASE_SCHEMA", "UPPER_CASE_TABLE");

        DbRow<DbRowValue> row = rowHelper.mixedCaseRow(1);

        testDb.createTableFromRow(row, lowerCaseTable);
        testDb.createTableFromRow(row, mixedCaseTable);
        testDb.createTableFromRow(row, upperCaseTable);

        testDb.insertRowIntoTable(row, lowerCaseTable);
        testDb.insertRowIntoTable(row, mixedCaseTable);
        testDb.insertRowIntoTable(row, upperCaseTable);

//        updateAndAssertOutput(
//                doNothingOnIncrementalUpdate(),
//                newState(),
//                ImmutableList.of(lowerCaseTable, mixedCaseTable, upperCaseTable));
    }

    @Test
    @Ignore
    public void sync_incrementalUpdate_shouldHandleAnyCase() throws Exception {
        TableRef lowerCaseTable = tableHelper.customTable("lower_case_schema", "lower_case_table");
        TableRef mixedCaseTable = tableHelper.customTable("MiXeD_CaSe_ScHeMa", "MiXeD_CaSe_TaBlE");
        TableRef upperCaseTable = tableHelper.customTable("UPPER_CASE_SCHEMA", "UPPER_CASE_TABLE");

        DbRow<DbRowValue> row = rowHelper.mixedCaseRow(1);

        testDb.createTableFromRow(row, lowerCaseTable);
        testDb.createTableFromRow(row, mixedCaseTable);
        testDb.createTableFromRow(row, upperCaseTable);

        // configure state - to avoid DDL change logs, we do this after table creation
//        State state = incrementalUpdateState(ImmutableList.of(lowerCaseTable, mixedCaseTable, upperCaseTable));

        testDb.insertRowIntoTable(row, lowerCaseTable);
        testDb.insertRowIntoTable(row, mixedCaseTable);
        testDb.insertRowIntoTable(row, upperCaseTable);

//        updateAndAssertOutput(
//                failOnImportTable(), state, ImmutableList.of(lowerCaseTable, mixedCaseTable, upperCaseTable));
    }

    @Test
    @Ignore
    public void prepareSync() throws SQLException {
        // arrange
        DbRow<DbRowValue> row = rowHelper.primaryKeylessRow(1);

        TableRef importStarted = tableHelper.customTable("import_started");
        TableRef importNotStarted = tableHelper.customTable("import_not_started");
        TableRef excludedTableRef = tableHelper.customTable("excluded_table");

        testDb.createTableFromRow(row, importStarted);
        testDb.createTableFromRow(row, importNotStarted);
        testDb.createTableFromRow(row, excludedTableRef);

        Set<TableRef> includedTables = ImmutableSet.of(importNotStarted, importStarted);
        Set<TableRef> excludedTables = ImmutableSet.of(excludedTableRef);

        // TableDefinitions have the destination prefix added. Get around this by comparing the table names only.
        Set<String> expectedAsserted = ImmutableSet.of(importNotStarted.name);
        Set<String> expectedNotAsserted = ImmutableSet.of(importStarted.name, excludedTableRef.name);

        State state = midImportState(ImmutableSet.of(importStarted), ImmutableSet.of(importNotStarted));
//        Service spyService = spy(createService());

        ArgumentCaptor<TableDefinition> tableDefCaptor = ArgumentCaptor.forClass(TableDefinition.class);
        MockOutput2<State> mockedMockoutput = (MockOutput2<State>) mock(MockOutput2.class);
//        doNothing().when(mockedMockoutput).assertTableSchema(tableDefCaptor.capture());

        // act
//        executePrepareSync(mockedMockoutput, spyService, state, includedTables, excludedTables);

        // assert
//        boolean correctTablesAsserted =
//                tableDefCaptor
//                        .getAllValues()
//                        .stream()
//                        .map(td -> td.tableRef.name)
//                        .allMatch(
//                                tableName ->
//                                        expectedAsserted.contains(tableName)
//                                                && !expectedNotAsserted.contains(tableName));
//        assertTrue(correctTablesAsserted);
//        verify(spyService).prepareSync(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @Ignore
    public void standardConfig() {

//        Service service = createService();
//        State state = newState();
//
//        StandardConfig config = service.standardConfig(testDb.credentials(), params(), state);
//        assertTrue(config.isSupportsExclude());
//        assertFalse(config.isExcludeByDefault());
    }

    @Test
    @Ignore
    public void stdConfig_SupportsColumnConfigByDefault() throws SQLException {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "test");

        TableRef tableRef = new TableRef(defaultTable.schema, "test_table");
        testDb.createTableFromRow(row, tableRef);

//        Service service = createService();
//        State state = newState();
//
//        StandardConfig config = service.standardConfig(testDb.credentials(), params(), state);
//        assertTrue(config.getSchema(tableRef.schema).getTable(tableRef.name).isSupportsColumnConfig());
    }

    @Test
    @Ignore
    public void stdConfig_ConflictingNamesAreSetExcluded() throws SQLException {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        TableRef tableWithUnderscore = new TableRef(defaultTable.schema, "my_table");
        TableRef tableWithDollar = new TableRef(defaultTable.schema, "my$table");

        testDb.createTableFromRow(row, tableWithUnderscore);
        testDb.createTableFromRow(row, tableWithDollar);

//        Service service = createService();
//        State state = newState();

//        StandardConfig config = service.standardConfig(testDb.credentials(), params(), state);

//        assertTrue(
//                "Table with conflicting names must be set as excluded by system",
//                config.getSchema(tableWithUnderscore.schema)
//                        .getTable(tableWithUnderscore.name)
//                        .getExcludedBySystem()
//                        .isPresent());
//        assertTrue(
//                "Table with conflicting names must be set as excluded by system",
//                config.getSchema(tableWithDollar.schema)
//                        .getTable(tableWithDollar.name)
//                        .getExcludedBySystem()
//                        .isPresent());
    }
}
