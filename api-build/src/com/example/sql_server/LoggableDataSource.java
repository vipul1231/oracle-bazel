package com.example.sql_server;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.*;
import java.util.logging.Logger;
import javax.sql.DataSource;

/** Wrapper for DataSource & TunnelDataSource. Use to print queries to customer */
public class LoggableDataSource implements DataSource, AutoCloseable {

    private final DataSource wrappedDataSource;
    public final LoggableConfig config; // change this back to private once `TimedOutLoggableDataSource` is gone

    public LoggableDataSource(DataSource dataSource, boolean writesToCustomerLogs) {
        this(dataSource, new LoggableConfig.Builder().setWritesToCustomerLogs(writesToCustomerLogs).build());
    }

    public LoggableDataSource(DataSource dataSource) {
        this(dataSource, LoggableConfig.defaultConfig());
    }

    public LoggableDataSource(DataSource dataSource, LoggableConfig config) {
        this.wrappedDataSource = dataSource;
        this.config = config;
    }

    @Override
    public LoggableConnection getConnection() throws SQLException {
        return new LoggableConnection(wrappedDataSource.getConnection(), config);
    }

    @Override
    public LoggableConnection getConnection(String username, String password) throws SQLException {
        return new LoggableConnection(wrappedDataSource.getConnection(username, password), config);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrappedDataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrappedDataSource.isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return wrappedDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        wrappedDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        wrappedDataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return wrappedDataSource.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return wrappedDataSource.getParentLogger();
    }

    @Override
    public void close() {
        try {
            if (wrappedDataSource instanceof AutoCloseable) ((AutoCloseable) wrappedDataSource).close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
