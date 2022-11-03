package com.example.oracle.verification;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class OracleTLSWrapperDataSource implements DataSource, AutoCloseable {
    private final DataSource wrappedDataSource;
    private SSLContext tlsSSLContext;
    private SSLContext defaultSSLContext;

    public OracleTLSWrapperDataSource(DataSource dataSource, TrustManager trustManager) {
        this.wrappedDataSource = dataSource;
        try {
            defaultSSLContext = SSLContext.getInstance("SSL");
            defaultSSLContext.init(null, null, null);

            tlsSSLContext = SSLContext.getInstance("SSL");
            tlsSSLContext.init(null, new TrustManager[] {trustManager}, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {

        SSLContext.setDefault(tlsSSLContext);
        try {

            Connection conn = wrappedDataSource.getConnection();
            return conn;
        } finally {
            SSLContext.setDefault(defaultSSLContext);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {

        SSLContext.setDefault(tlsSSLContext);
        try {
            Connection conn = wrappedDataSource.getConnection(username, password);
            return conn;
        } finally {
            SSLContext.setDefault(defaultSSLContext);
        }
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