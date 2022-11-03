package com.example.snowflakecritic;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SnowflakeSession {

    public enum SnowflakeParameter {
        TIMESTAMP_OUTPUT_FORMAT,
        TIMEZONE
    }

    private static final Map<SnowflakeParameter, String> SESSION_PARAMS = new HashMap<>();

    static {
        SESSION_PARAMS.put(SnowflakeParameter.TIMESTAMP_OUTPUT_FORMAT, "YYYY-MM-DD HH24:MI:SS.FF9");
        SESSION_PARAMS.put(SnowflakeParameter.TIMEZONE, "UTC");
    }

    private static final String SET_PARAM_Q = "ALTER SESSION set %s = '%s'";

    public Connection configureSession(Connection connection) throws SQLException {

        for (Map.Entry<SnowflakeParameter, String> param : SESSION_PARAMS.entrySet()) {
            executeSql(connection, String.format(SET_PARAM_Q, param.getKey().name(), param.getValue()));
        }

        return connection;
    }

    public void executeSql(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
