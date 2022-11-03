package com.example.snowflakecritic;

import com.example.core2.ConnectionParameters;

import java.sql.Connection;
import java.sql.SQLException;


public class SnowflakeSourceWithSession extends SnowflakeSource {

    private final SnowflakeSession session = new SnowflakeSession();

    public SnowflakeSourceWithSession(SnowflakeSourceCredentials originalCredentials, ConnectionParameters params) {
        super(originalCredentials, params);
    }

    @Override
    public Connection connection() throws SQLException {
        return session.configureSession(super.connection());
    }
}
