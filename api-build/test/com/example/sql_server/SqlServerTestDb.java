package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.TableRef;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.example.fire.migrated.sql_server.dockerized.MigratedDockerizedSqlServerDatabase;
import com.example.metal.shared.FireDockerImage;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlServerTestDb extends TestSourceDb<DbCredentials> {

    private MigratedDockerizedSqlServerDatabase dockerizedDb;

    @FunctionalInterface
    public interface FunctionToExecuteQuery<T> {
        T execute(Connection connection) throws SQLException;
    }

    public SqlServerTestDb() {
        super(new SqlServerRowHelper());
        try {
            this.dockerizedDb = new MigratedDockerizedSqlServerDatabase(FireDockerImage.SQL_SERVER_2019_CU10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String database() {
        return adminCredentials().database.orElseThrow(() -> new RuntimeException("Database required"));
    }

    @Override
    public void execute(String... sql) throws SQLException {
        execute(dataSource(), sql);
    }

    @Override
    public void executeAsLimitedUser(String... sql) throws SQLException {
        execute(dataSource(), sql);
    }

    private void execute(DataSource dataSource, String... sql) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
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

    public <T> T execute(FunctionToExecuteQuery<T> function) throws SQLException {
        try (Connection c = dataSource().getConnection()) {
            return function.execute(c);
        }
    }

    @Override
    public DbCredentials adminCredentials() {
        return dockerizedDb.getAdminCredentials();
    }

    @Override
    public DbCredentials credentials() {
        return dockerizedDb.getCredentials();
    }

    @Override
    protected DataSource adminDataSource() {
        return dockerizedDb.getAdminDataSource();
    }

    @Override
    public DataSource dataSource() {
        return dockerizedDb.getDataSource();
    }

    @Override
    public String quote(String identifier) {
        return '[' + identifier.replace("]", "]]") + ']';
    }

    @Override
    public void createTableFromRow(DbRow<DbRowValue> row, TableRef tableRef) throws SQLException {
        createTableFromRow(row, tableRef, true);
    }

    public void createTableFromRow(DbRow<DbRowValue> row, TableRef tableRef, boolean useChangeTracking)
            throws SQLException {
        super.createTableFromRow(row, tableRef);

        // Enable change tracking to new table
        if (useChangeTracking && row.stream().anyMatch(r -> r.primaryKey)) {
            execute("ALTER TABLE " + quote(tableRef) + " ENABLE CHANGE_TRACKING");
            execute("GRANT VIEW CHANGE TRACKING ON " + quote(tableRef) + " TO " + credentials().user);
        } else {
            execute(
                    String.format(
                            "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL",
                            tableRef.schema, tableRef.name));
        }
    }

    @Override
    public void createSchemaIfNotExists(String schema) throws SQLException {
        try {
            execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            if (e.getErrorCode() != 2714) throw e;
        }
    }

    @Override
    public TableRef uniqueTable(String tableIdentifier) {
        return new TableRef(MigratedDockerizedSqlServerDatabase.TEST_SCHEMA, tableIdentifier + "_" + randomSuffix());
    }

    @Override
    public void close() {
        credentials().database.ifPresent(dockerizedDb::dropDatabase);
    }

    public void release() throws Exception {
        dockerizedDb.close();
    }

    public void disableCDC(TableRef tableRef) throws SQLException {
        execute(
                String.format(
                        "EXEC sys.sp_cdc_disable_table @source_schema = N'%s', @source_name = N'%s', @capture_instance = 'all'",
                        tableRef.schema, tableRef.name));
    }
}
