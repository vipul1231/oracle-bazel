package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.google.common.base.Preconditions;
import net.snowflake.client.jdbc.SnowflakeResultSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

//public class SnowflakeReplicationTableUpdater extends AbstractSnowflakeTableWorker implements SnowflakeTableUpdater {
public class SnowflakeReplicationTableUpdater {
        private SnowflakeTableInfo tableInfo;
    private SnowflakeConnectorState.SnowflakeTableState tableState;
    private SnowflakeColumnInfo replicationKey;
    private Set<String> queryIDs;

    public SnowflakeReplicationTableUpdater(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef, SnowflakeColumnInfo replicationKey) {
        //super(serviceConfig, tableRef);
        tableInfo = serviceConfig.getSnowflakeInformer().tableInfo(tableRef);
//        tableState = getSnowflakeConnectorState().getTableState(tableRef);
        this.replicationKey = replicationKey;
        queryIDs = new HashSet<>();
    }

    //@Override
    public void incrementalUpdate() {
        System.out.println("Start incremental update for " + tableInfo.sourceTable + " with replication key: " + replicationKey.columnName);
        //Preconditions.checkArgument(tableState.maxReplicationKey != null, "Max replication key is not set");
        //final Comparable[] maxRelicationKey = new Comparable[] { tableState.maxReplicationKey };
        //execute(resultSet -> {
            /*while (resultSet.next()) {
                Comparable key = (Comparable) resultSet.getObject(replicationKey.columnName);
                getOutput().upsert(tableInfo.tableDefinition(),
                        extractRowValues(
                                tableInfo,
                                tableInfo.includedColumnInfo(),
                                sourceValueFetcher(resultSet),
                                false));
                Preconditions.checkArgument(key != null);
                if (key.compareTo(maxRelicationKey[0]) > 0) {
                    maxRelicationKey[0] = key;
                }
            }*/
        //}, "select * from %s where %s > %s",
        //        tableInfo.sourceTable,
         //       replicationKey.columnName);
    }

//    public void execute(SQLConsumer<ResultSet> resultSetSQLConsumer, String format, Object... args) {
//        String sql = String.format(format, args);
//        System.out.println("Execute SQL: " + sql);
//        try (Connection connection = serviceConfig.getSnowflakeSource().connection();
//             Statement statement = connection.createStatement();
//             ResultSet resultSet = statement.executeQuery(sql)) {
//            queryIDs.add(resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
//            resultSetSQLConsumer.accept(resultSet);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public Set<String> queryIDs() {
        return queryIDs;
    }

    @FunctionalInterface
    interface SQLConsumer<T> {
        public void accept(T input) throws SQLException;
    }
}
