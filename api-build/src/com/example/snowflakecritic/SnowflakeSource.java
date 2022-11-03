package com.example.snowflakecritic;

import com.example.core.DbCredentials;
import com.example.db.SqlDbSource;
import com.example.core2.ConnectionParameters;

import net.snowflake.client.jdbc.SnowflakeStatement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class SnowflakeSource extends SqlDbSource<SnowflakeSourceCredentials> {
    private static final DataSourceInitRetrier RETRIER = new DataSourceInitRetrier();

    /** Snowflake does not support any ConnectionParameters, so we can ignore that field */
    public SnowflakeSource(SnowflakeSourceCredentials originalCredentials, ConnectionParameters params) {
        super(originalCredentials, params);
    }
    /** Use data writer credentials (from core) to build a SnowflakeSource * */
    public SnowflakeSource(SnowflakeCredentials snowflakeCredentials) {
        super(SnowflakeSourceCredentials.fromSnowflakeCredentials(snowflakeCredentials), null);
    }

    public String getDatabase() {
        return credentials.database.get();
    }

    public ConnectionParameters getConnectionParameters() {
        return params;
    }

    @Override
    protected DataSource nonLoggableDataSource(
            SnowflakeSourceCredentials originalCredentials, DbCredentials __, ConnectionParameters ___) {
        Callable<DataSource> dataSourceInit =
                () -> {
            return SnowflakeConnect.INSTANCE.connectDirectly(originalCredentials.toSnowflakeCredentials()) ;
        };

        try {
            return RETRIER.get(dataSourceInit, 3);
        } catch (Exception e) {
            e.printStackTrace();
            //DbRetrier.FinalExceptionThrower<RuntimeException> finalExceptionThrower = baseFinalExceptionThrower();

            //e.getAttemptFailures().forEach(e1 -> finalExceptionThrower.accept(e));
            // throw a RuntimeException here in case finalExceptionThrower is ineffective
            throw new RuntimeException(e);
        }
    }


    public void drop(String tableName) {
        update("drop table if exists %s", tableName);
    }

    public void make(SnowflakeTableInfo tableInfo) {
        update("create or replace table public.%s (%s)",
                tableInfo.sourceTable,
                tableInfo.includedColumnInfo().stream().map(
                        c -> c.columnName + " " + c.sourceType.name())
                        .collect(Collectors.joining(",")));
    }

    public void delete(SnowflakeTableInfo tableInfo) {
        update("delete from %s", tableInfo.sourceTable);
    }

    public <T> T execute(
            SnowflakeTimeTravelTableImporter.SQLFunction<ResultSet, T> resultSetSQLFunction,
            String format,
            Object... args) {
        String sql = String.format(format, args);
        System.out.println(sql);
        try (Connection connection = connection();
             java.sql.Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSetSQLFunction.apply(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void consume(SnowflakeTimeTravelTableImporter.SQLConsumer<java.sql.Statement> statementConsumer) {
        try (Connection connection = connection();
             java.sql.Statement statement = connection.createStatement(); ) {
            statementConsumer.accept(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String update(String format, Object... args) {
        String sql = String.format(format, args);
        System.out.println("Update: " + sql);
        try (Connection connection = connection(); java.sql.Statement statement = connection.createStatement(); ) {
            statement.executeUpdate(sql);
            return statement.unwrap(SnowflakeStatement.class).getQueryID();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
