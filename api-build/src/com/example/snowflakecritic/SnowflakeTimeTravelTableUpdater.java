package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.google.common.base.Preconditions;
import net.snowflake.client.jdbc.SnowflakeResultSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SnowflakeTimeTravelTableUpdater extends AbstractSnowflakeTableUpdater {
    private SnowflakeTableInfo tableInfo;
    SnowflakeConnectorState.SnowflakeTableState tableState;
    private Set<String> queryIDs;
    private String currentSystemTimestamp;


    public SnowflakeTimeTravelTableUpdater(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
        queryIDs = new HashSet<>();
        tableInfo = serviceConfig.getSnowflakeInformer().tableInfo(tableRef);
        tableState = getSnowflakeConnectorState().getTableState(tableRef);
    }

    @Override
    public SnowflakeSourceCredentials.UpdateMethod getUpdateMethod() {
        return SnowflakeSourceCredentials.UpdateMethod.TIME_TRAVEL;
    }

    @Override
    protected String getTimeForLastSync() {
        return currentSystemTimestamp;
    }

    @Override
    public void performPreImport() {

    }

    public Set<String> queryIDs() {
        return queryIDs;
    }

    @Override
    protected void doPerformIncrementalUpdate() {
        SnowflakeTableInfo tableInfo = serviceConfig.getSnowflakeInformer().tableInfo(tableRef);
        Preconditions.checkArgument(tableState.lastSyncTime != null, "Run import first on table " + tableRef);

        String currentSystemTimestamp = getSnowflakeSystemInfo().getSystemTime();

        boolean result =
                execute(
                        resultSet -> {
                            ResultSetHelper resultSetHelper = new ResultSetHelper(tableInfo, resultSet);
                            while (resultSet.next()) {
                                submitRow(resultSetHelper.extractRowValues());
                            }
                            return true;
                        },
                        "select * from %s changes(information => default) at(timestamp => to_timestamp_tz('%s', '%s')) end(timestamp => to_timestamp_tz('%s', '%s'))",
                        tableInfo.sourceTable,
                        tableState.lastSyncTime,
                        SnowflakeTimeTravelTableImporter.SNOWFLAKE_TIMESTAMP_FORMAT,
                        currentSystemTimestamp,
                        SnowflakeTimeTravelTableImporter.SNOWFLAKE_TIMESTAMP_FORMAT);

        tableState.updateCompleted();
        tableState.lastSyncTime = currentSystemTimestamp;
        checkpoint();
    }

    public <T> T execute(SQLFunction<ResultSet, T> resultSetSQLFunction, String format, Object... args) {
        String sql = String.format(format, args);
        System.out.println("Execute SQL: " + sql);
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
    interface SQLFunction<T, F> {
        public F apply(T input) throws SQLException;
    }

    @Override
    protected OperationTag[] getOperationTags() {
        return new OperationTag[] {OperationTag.INCREMENTAL_SYNC};
    }
}