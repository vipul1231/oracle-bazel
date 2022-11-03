package com.example.sql_server;

import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.example.core.DbCredentials;
import com.example.core.TableRef;
import com.example.db.DbRow;
import com.example.db.DbRowValue;

import javax.sql.DataSource;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;
import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;

public abstract class TestSourceDb<Creds extends DbCredentials> implements AutoCloseable {

    protected final DbRowHelper rowHelper;

    protected abstract Creds adminCredentials();

    public abstract Creds credentials();

    protected abstract DataSource adminDataSource();

    public abstract DataSource dataSource();

    public TestSourceDb(DbRowHelper rowHelper) {
        this.rowHelper = rowHelper;
    }

    public void execute(String... sql) throws SQLException {
        execute(__ -> {
        }, sql);
    }

    public void executeAsAdmin(String... sql) throws SQLException {
        executeAsAdmin(__ -> {
        }, sql);
    }

    public String user() {
        return credentials().user;
    }

    /**
     * Override to escape string identifiers (e.g. column/table names) in the manner required by each database.
     */
    public abstract String quote(String identifier);

    public String quote(TableRef tableRef) {
        return quote(tableRef.schema) + "." + quote(tableRef.name);
    }

    protected static String randomSuffix() {
        return UUID.randomUUID().toString().substring(0, 3);
    }

    static int SHORT_COLUMN_LENGTH = 1;

    public interface SqlConsumer<T> {
        void accept(T value) throws SQLException;
    }

    public void execute(SqlConsumer<Connection> applyConnectionSettings, String... sql) throws SQLException {
        try (Connection c = dataSource().getConnection()) {
            applyConnectionSettings.accept(c);
            for (String s : sql) {
                System.out.println("Executing query: " + s);
                try (Statement stmt = c.createStatement()) {
                    stmt.execute(s);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public void executeAsAdmin(SqlConsumer<Connection> applyConnectionSettings, String... sql) throws SQLException {
        try (Connection c = adminDataSource().getConnection()) {
            applyConnectionSettings.accept(c);
            for (String s : sql) {
                System.out.println("Executing query: " + s);
                try (Statement stmt = c.createStatement()) {
                    stmt.execute(s);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    protected static String randomString(int length) {
        byte[] array = new byte[length];
        new Random().nextBytes(array);
        return new String(array, Charset.forName("UTF-8")).replaceAll("[^A-Za-z]", "a");
    }

    public void createAndFillVeryWideTable(TableRef tableRef, int totalRows) throws Exception {
        createAndFillVeryWideTable(tableRef, totalRows, SHORT_COLUMN_LENGTH);
    }

    public void createAndFillVeryWideTable(TableRef tableRef, int totalRows, int stringLength) throws Exception {
        createAndFillTable(tableRef, totalRows, stringLength, Math.min(1000, rowHelper.maxColumnsPerTable() - 2));
    }

    public void createAndFillSimpleTable(TableRef tableRef, int totalRows) throws Exception {
        createAndFillSimpleTable(tableRef, totalRows, SHORT_COLUMN_LENGTH);
    }

    public void createAndFillSimpleTable(TableRef tableRef, int totalRows, int stringLength) throws Exception {
        createAndFillTable(tableRef, totalRows, stringLength, 1);
    }

    public void createAndFillTable(TableRef tableRef, int totalRows, int stringLength, int numSimpleColumns)
            throws Exception {
        List<DbRow<DbRowValue>> rows = new ArrayList<>();

        for (int i = 1; i <= totalRows; i++) {
            DbRow<DbRowValue> row = rowHelper.simpleRow(i, randomString(stringLength));
            for (int colId = numSimpleColumns; colId > 1; colId--) {
                row.add(rowHelper.simpleStringColumn("col" + colId, randomString(stringLength)));
            }
            rows.add(row);

            if (i == 1) {
                createTableFromRow(rows.get(0), tableRef);
            }

            if ((rows.size() * numSimpleColumns * stringLength) > 100_000_000) { // data is probably more 100MB in size
                insertRowsIntoTable(tableRef, rows);
                rows.clear();
            }
        }

        if (rows.size() > 0) {
            insertRowsIntoTable(tableRef, rows);
        }
    }

    public abstract void release() throws Exception;

    public void dropTableIfExists(TableRef tableRef) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void dropSchemaIfExists(String schema) throws SQLException {
        execute("DROP SCHEMA IF EXISTS " + quote(schema));
    }

    public void createSchemaIfNotExists(String schema) throws SQLException {
        execute("CREATE SCHEMA IF NOT EXISTS " + quote(schema));
    }

    public void createTableFromRow(DbRow<DbRowValue> row, TableRef tableRef) throws SQLException {
        createSchemaIfNotExists(tableRef.schema);

        String columnDefinitions =
                row.stream()
                        .map(rv -> quote(rv.columnName) + " " + rv.columnType + " " + rv.columnDescriptor.orElse(""))
                        .collect(Collectors.joining(", "));

        String pKeyDefinitions =
                row.stream().filter(rv -> rv.primaryKey).map(rv -> rv.columnName).collect(Collectors.joining(", "));

        pKeyDefinitions = pKeyDefinitions.isEmpty() ? pKeyDefinitions : "PRIMARY KEY (" + pKeyDefinitions + ")";

        String fKeyDefinitions =
                row.stream()
                        .filter(rv -> rv.referencedTableRef.isPresent() && rv.referencedColumn.isPresent())
                        .map(
                                rv ->
                                        "FOREIGN KEY ("
                                                + quote(rv.columnName)
                                                + ") REFERENCES "
                                                + quote(rv.referencedTableRef.get())
                                                + " ("
                                                + quote(rv.referencedColumn.get())
                                                + ")")
                        .collect(Collectors.joining(", "));

        String tableDefinition =
                Stream.of(columnDefinitions, pKeyDefinitions, fKeyDefinitions)
                        .filter(def -> !def.isEmpty())
                        .collect(Collectors.joining(", "));

        execute("CREATE TABLE " + quote(tableRef) + " (" + tableDefinition + ")");

//        await().atMost(60, TimeUnit.SECONDS).until(() -> tableExists(tableRef));
    }

    protected abstract void executeAsLimitedUser(String... sql) throws SQLException;

    /**
     * Override if you need to change the initial wait conditions
     *
     * @return ConditionFactory
     */
    protected ConditionFactory await() {
//        return Awaitility.await();
        return null;
    }

    private Boolean tableExists(TableRef tableRef) {
        try {
            execute("SELECT 1 FROM " + quote(tableRef));
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public void insertRowIntoTable(DbRow<DbRowValue> row, TableRef tableRef) throws SQLException {
        insertRowsIntoTable(tableRef, Collections.singletonList(row));
    }

    public void insertRowsIntoTable(TableRef tableRef, List<DbRow<DbRowValue>> rows) throws SQLException {
        Predicate<DbRowValue> inputValueIsValid = rv -> rv.inputValue != null && !rv.inputValue.toString().isEmpty();

        String colNames =
                rows.isEmpty()
                        ? ""
                        : "("
                        + rows.get(0)
                        .stream()
                        .filter(inputValueIsValid)
                        .map(rv -> rv.columnName)
                        .collect(Collectors.joining(", "))
                        + ")";

        List<String> rowValuesAsStrings = new ArrayList<>();

        for (DbRow<DbRowValue> row : rows) {
            rowValuesAsStrings.add(
                    "("
                            + row.stream()
                            .filter(inputValueIsValid)
                            .map(rv -> rv.inputValue.toString())
                            .collect(Collectors.joining(", "))
                            + ")");

            if (rowValuesAsStrings.size() > 1000) {
                // This is so we don't run out of memory. `rowValuesAsStrings` can be big.
                execute(
                        "INSERT INTO "
                                + quote(tableRef)
                                + " "
                                + colNames
                                + " VALUES "
                                + String.join(", ", rowValuesAsStrings));
                rowValuesAsStrings.clear();
            }
        }

        if (rowValuesAsStrings.size() > 0) {
            execute(
                    "INSERT INTO "
                            + quote(tableRef)
                            + " "
                            + colNames
                            + " VALUES "
                            + String.join(", ", rowValuesAsStrings));
        }
    }

    public void updateSimpleRowWithKey(TableRef tableRef, int key, String value) throws SQLException {
        updateRow(tableRef, UPDATABLE_COLUMN_NAME, value, PRIMARY_KEY_NAME, key);
    }

    public void deleteSimpleRowWithKey(TableRef tableRef, int key) throws SQLException {
        deleteRow(tableRef, PRIMARY_KEY_NAME, key);
    }

    public void deleteRow(TableRef tableRef, String whereColumn, Object whereValue) throws SQLException {
        deleteRow(tableRef, Collections.singletonList(whereColumn), Collections.singletonList(whereValue));
    }

    public void deleteRow(TableRef tableRef, List<String> whereColumns, List<Object> whereValues) throws SQLException {
        execute("DELETE FROM " + quote(tableRef) + whereClauseEquals(whereColumns, whereValues));
    }

    public void updateRow(
            TableRef tableRef, String updateColumn, Object updateValue, String whereColumn, Object whereValue)
            throws SQLException {

        execute(
                "UPDATE "
                        + quote(tableRef)
                        + " SET "
                        + updateColumn
                        + " = '"
                        + updateValue
                        + "' WHERE "
                        + whereColumn
                        + " = "
                        + whereValue);
    }

    public void updateSimpleRowAll(TableRef tableRef, String value) throws Exception {
        execute("UPDATE " + quote(tableRef) + " SET " + UPDATABLE_COLUMN_NAME + " = '" + value + "'");
    }

    public void updateSimpleRowMultipleCols(
            TableRef tableRef, Map<String, Object> values, String whereColumn, Object whereValue) throws SQLException {
        updateSimpleRowMultipleCols(
                tableRef, values, Collections.singletonList(whereColumn), Collections.singletonList(whereValue));
    }

    public void updateSimpleRowMultipleCols(
            TableRef tableRef, Map<String, Object> values, List<String> whereColumns, List<Object> whereValues)
            throws SQLException {
        StringBuilder queryBuilder = new StringBuilder(String.format("UPDATE %s SET ", quote(tableRef)));
        int count = 0;
        for (Map.Entry<String, Object> value : values.entrySet()) {
            queryBuilder.append(value.getKey()).append(" = ").append(value.getValue());
            if (++count != values.size()) queryBuilder.append(", ");
        }
        queryBuilder.append(whereClauseEquals(whereColumns, whereValues));
        execute(queryBuilder.toString());
    }

    @Deprecated
    public TableRef uniqueTable(String tableIdentifier) {
        String database =
                credentials().database.orElseThrow(() -> new RuntimeException("Expected database in " + credentials()));
        return new TableRef(database, tableIdentifier + "_" + randomSuffix());
    }

    protected String escapeStringValue(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    protected String whereClauseEquals(List<String> whereColumns, List<Object> whereValues) {
        StringBuilder whereClause = new StringBuilder(" WHERE ");
        for (int i = 0; i < whereColumns.size(); i++) {
            whereClause
                    .append(whereColumns.get(i))
                    .append(" = ")
                    .append(escapeStringValue(whereValues.get(i).toString()));
            if (i + 1 != whereColumns.size()) whereClause.append(" AND ");
        }
        return whereClause.toString();
    }
}
