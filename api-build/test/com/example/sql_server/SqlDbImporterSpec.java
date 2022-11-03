package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbImporter;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.example.oracle.ColumnConfig;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import static com.example.db.DbRowValue.FOREIGN_KEY_NAME;
import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;

public abstract class SqlDbImporterSpec<Importer extends DbImporter<TableRef>, SqlCreds extends DbCredentials, State>
        extends DbImporterSpec<Importer, SqlCreds, State> {

    public TableRef defaultTable = new TableRef("test", "test");

    @Override
    protected SqlDbTableHelper tableHelper() {
        return new SqlDbTableHelper();
    }

    protected abstract State newState();

    protected abstract SqlServerState preparePreImportState(SqlServerState newState, TableRef tableToImport);

    protected abstract Class<State> stateClass();

    protected abstract Importer createImporter(State state, Output<State> output, StandardConfig config);

    @Ignore
    @Test
    public void importPage_foreignKey() throws SQLException, IOException {
        TableRef parentTable = tableHelper.customTable("parent");
        TableRef childTable = tableHelper.customTable("child");

        DbRow<DbRowValue> parentRow = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> childRow = rowHelper.simpleRow(2, "bar");

//        childRow.add(rowHelper.foreignKeyRefRowValue(FOREIGN_KEY_NAME, 1, parentTable, PRIMARY_KEY_NAME));

        testDb.createTableFromRow(parentRow, parentTable);
        testDb.createTableFromRow(childRow, childTable);

        testDb.insertRowIntoTable(parentRow, parentTable);
        testDb.insertRowIntoTable(childRow, childTable);

//        State preImportState = preparePreImportState(childTable);
//        importPageAndAssertOutput(preImportState, childTable, assertImportFinished(childTable));
    }

    @Ignore
    @Test
    public void importPage_foreignKeys() throws SQLException, IOException {
        TableRef parentTableA = tableHelper.customTable("parent_a");
        TableRef parentTableB = tableHelper.customTable("parent_b");
        TableRef childTable = tableHelper.customTable("child");

        DbRow<DbRowValue> parentRowA = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> parentRowB = rowHelper.simpleRow(2, "bar");
        DbRow<DbRowValue> childRow = rowHelper.simpleRow(3, "baz");

        childRow.add(rowHelper.foreignKeyRefRowValue(FOREIGN_KEY_NAME + "_A", 1, parentTableA, PRIMARY_KEY_NAME));
        childRow.add(rowHelper.foreignKeyRefRowValue(FOREIGN_KEY_NAME + "_B", 2, parentTableB, PRIMARY_KEY_NAME));

        testDb.createTableFromRow(parentRowA, parentTableA);
        testDb.createTableFromRow(parentRowB, parentTableB);
        testDb.createTableFromRow(childRow, childTable);

        testDb.insertRowIntoTable(parentRowA, parentTableA);
        testDb.insertRowIntoTable(parentRowB, parentTableB);
        testDb.insertRowIntoTable(childRow, childTable);

//        State preImportState = preparePreImportState(childTable);
//        importPageAndAssertOutput(preImportState, childTable, assertImportFinished(childTable));
    }

    @Ignore
    @Test
    public void importPage_compoundPrimaryKey() throws SQLException, IOException {
//        DbRow<DbRowValue> compoundPKeyRow = rowHelper.compoundPrimaryKeyRow(1, 2);
//
//        testDb.createTableFromRow(compoundPKeyRow, defaultTable);
//        testDb.insertRowIntoTable(compoundPKeyRow, defaultTable);
//
//        State preImportState = preparePreImportState(defaultTable);
//        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_shouldContainOnlySelectedColumns() throws SQLException {
        // arrange

        String selectedColumnName = "selected_column";
        String userExcludedColumnName = "user_excluded_column";
        String systemExcludedColumnName = "system_excluded_column";

        int primaryKeyValue = 1;
        int selectedColumnValue = 2;

        DbRow<DbRowValue> row =
                new DbRow<>(
                        new DbRowValue.Builder("INT", PRIMARY_KEY_NAME).value(primaryKeyValue).primaryKey(true).build(),
                        new DbRowValue.Builder("INT", selectedColumnName).value(selectedColumnValue).build(),
                        new DbRowValue.Builder("INT", userExcludedColumnName).value(3).build(),
                        new DbRowValue.Builder("INT", systemExcludedColumnName).value(4).build());

        testDb.createTableFromRow(row, defaultTable);
        testDb.insertRowIntoTable(row, defaultTable);

//        State preImportState = preparePreImportState(defaultTable);
//        Map<String, ColumnConfig> columns = new HashMap<>();
//
//        ColumnConfig primaryKeyColumn = new ColumnConfig();
//        primaryKeyColumn.setExclusionNotSupported(Optional.of("Can't exclude this column, it's a primary key"));
//
//        ColumnConfig userExcludedColumn = new ColumnConfig();
//        userExcludedColumn.setExcludedByUser(Optional.of(true));

        ColumnConfig systemExcludedColumn = new ColumnConfig();
        systemExcludedColumn.setExcludedBySystem(Optional.of("Sync this over my dead body"));

        // included columns don't even show up in standard config - only exclusions
//        columns.put(userExcludedColumnName, userExcludedColumn);
//        columns.put(systemExcludedColumnName, systemExcludedColumn);
//
//        TableConfig tableConfig = new TableConfig();
//        tableConfig.setColumns(Optional.of(columns));
//        SchemaConfig schemaConfig = new SchemaConfig();
//        schemaConfig.putTable(defaultTable.name, tableConfig);
//        StandardConfig standardConfig = new StandardConfig();
//        standardConfig.putSchema(defaultTable.schema, schemaConfig);
//        MockOutput2<State> spiedOut = Mockito.spy(new MockOutput2<>(preImportState));
//        Importer importer = createImporter(preImportState, spiedOut, standardConfig);

        // act
//        importer.importPage(defaultTable);
//        List<Map<String, Object>> all =
//                spiedOut.getAll().get(new TableRef("wh_prefix_" + defaultTable.schema, defaultTable.name));
//        Map<String, Object> outputRow = all.get(0);

        // assert
//        assertEquals(primaryKeyValue, outputRow.get(PRIMARY_KEY_NAME));
//        assertEquals(selectedColumnValue, outputRow.get(selectedColumnName));
//        assertNotNull(outputRow.get("_example_synced"));
//        assertNotNull(outputRow.get("_example_deleted"));
//        assertFalse(outputRow.containsKey(userExcludedColumnName));
//        assertFalse(outputRow.containsKey(systemExcludedColumnName));
//
//        assertEquals(4, outputRow.size());
    }

    protected abstract void limitPageSize(SqlServerImporter importerSpy, int maxRowsPerPage);

    protected abstract SqlServerState prepareMidImportState(
            SqlServerState newState, TableRef tableToImport, DbRow<DbRowValue> lastImportedRow);

    protected abstract SqlServerState preparePostImportState(SqlServerState newState, TableRef tableToImport);

    public abstract void tableSorter();
}

