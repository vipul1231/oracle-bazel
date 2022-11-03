package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.example.logger.ExampleLogger;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This importer class implements the SELECT [COLUMNS] FROM TABLE FUBAR with CHANGES clause strategy for importing
 * records.
 */
public class SnowflakeTimeTravelTableImporter extends AbstractSnowflakeTableImporter {
    public static final long DEFAULT_PAGE_SIZE = 5_000_000L;
    private static final String CLONE_FORMAT = "_FT_SNAPSHOT_%s";
    public static final String SNOWFLAKE_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9";
    private SnowflakeTableInfo tableInfo;
    private String snapshotTable;
    private Set<String> queryIDs;

    public SnowflakeTimeTravelTableImporter(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
        queryIDs = new HashSet<>();
        tableInfo = serviceConfig.getSnowflakeInformer().tableInfo(tableRef);
        this.snapshotTable = StringUtils.left(String.format(CLONE_FORMAT, tableRef.name), 255);
    }

    @Override
    protected String getImportMethodName() {
        return "TIME_TRAVEL";
    }

    @Override
    protected void beforeImport() {
        makeSnapshot();
    }

    @Override
    protected void afterImport() {
        removeSnapshot();
    }

    @Override
    protected String getTableQuery() {
        return SnowflakeSQLUtils.select(getSnowflakeInformer().tableInfo(tableRef).includedColumnInfo(), snapshotTable);
    }

    public Set<String> queryIDs() {
        return queryIDs;
    }

    private void makeSnapshot() {
        tableState.lastSyncTime = getSnowflakeSystemInfo().getSystemTime();

        LOG.info("Make snapshot: " + snapshotTable + " at time " + tableState.lastSyncTime);

        update(
                "CREATE OR REPLACE TABLE %S CLONE %S AT(TIMESTAMP => TO_TIMESTAMP_LTZ('%s', '%s'))",
                snapshotTable, tableRef.name, tableState.lastSyncTime, SNOWFLAKE_TIMESTAMP_FORMAT);
    }

    private void removeSnapshot() {
        LOG.info("Remove snapshot: " + snapshotTable);
        drop(snapshotTable);
    }

    public static String timestamp(LocalDateTime localDateTime) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").format(localDateTime);
    }

    /**
     * @param tableInfo
     * @return
     */
    public Optional<Long> getPageSize(SnowflakeTableInfo tableInfo) {
        return Optional.of(2L);
    }

    public <T> T execute(SQLFunction<ResultSet, T> resultSetSQLFunction, String sql) {
        try (Connection connection = serviceConfig.getSnowflakeSource().connection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            queryIDs.add(resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
            return resultSetSQLFunction.apply(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface SQLConsumer<T> {
        public void accept(T input) throws SQLException;
    }

    @FunctionalInterface
    public interface SQLFunction<T, F> {
        public F apply(T input) throws SQLException;
    }

    public static String formatter(String format, Object... args) {
        return String.format(format, args);
    }

    public void drop(String tableName) {
        update("DROP TABLE IF EXISTS %s", tableName);
    }

    public void make(SnowflakeTableInfo tableInfo) {
        update(
                "CREATE OR REPLACE TABLE %s (%s)",
                tableInfo.sourceTable,
                tableInfo
                        .includedColumnInfo()
                        .stream()
                        .map(c -> c.columnName + " " + c.sourceType.name())
                        .collect(Collectors.joining(",")));
    }

    public void delete(SnowflakeTableInfo tableInfo) {
        update("DELETE FROM %s", tableInfo.sourceTable);
    }

    public void consume(SnowflakeTimeTravelTableImporter.SQLConsumer<java.sql.Statement> statementConsumer) {
        try (Connection connection = getSnowflakeSource().connection();
             java.sql.Statement statement = connection.createStatement(); ) {
            statementConsumer.accept(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String update(String format, Object... args) {
        String sql = String.format(format, args);
        try (Connection connection = getSnowflakeSource().connection();
             java.sql.Statement statement = connection.createStatement(); ) {
            statement.executeUpdate(sql);
            return statement.unwrap(SnowflakeStatement.class).getQueryID();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected OperationTag[] getOperationTags() {
        return new OperationTag[] {OperationTag.HISTORICAL_SYNC};
    }
}
