package com.example.snowflakecritic;

import java.sql.*;
import java.time.Instant;
import java.util.function.Function;

public class SnowflakeSystemInfo {

    private final SnowflakeSource source;

    public SnowflakeSystemInfo(SnowflakeSource source) {
        this.source = source;
    }

    public String getSystemTime() {
        return source.execute(this::querySystemTime);
    }

    public String querySystemTime(Connection connection) throws SQLException {
        return executeSqlQuery(connection, "SELECT CURRENT_TIMESTAMP", this::getSystemTimeFromResultSet);
    }

    public static void logSessionInfo(Connection connection) throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()");
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                System.out.println("Current warehouse: " + resultSet.getString(1));
                System.out.println("Current database: " + resultSet.getString(2));
                System.out.println("Current schema: " + resultSet.getString(3));
                System.out.println("Current role: " + resultSet.getString(4));
            }
        }
    }

    private String getSystemTimeFromResultSet(ResultSet resultSet) {
        try {
            if (resultSet.next()) {
                return resultSet.getString(1);
            } else {
                throw new RuntimeException("No result from database");
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T executeSqlQuery(Connection connection, String sql, Function<ResultSet, T> resultSetConsumer)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            return resultSetConsumer.apply(resultSet);
        } catch (RuntimeException e) {
            if (e.getCause().getClass().isAssignableFrom(SQLException.class)) {
                throw (SQLException) e.getCause();
            }

            throw e;
        }
    }
}