package com.example.sql_server;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/** Wrapper for Connection. Used to print queries to customer logs */
public class LoggableConnection implements Connection {
    private final Connection wrappedConnection;
    public final LoggableConfig config; // change this back to private once `TimedOutLoggableDataSource` is gone

    public LoggableConnection(Connection connection, LoggableConfig config) {
        this.wrappedConnection = connection;
        this.config = config;
    }

    @Override
    public LoggableStatement createStatement() throws SQLException {
        return new LoggableStatement(wrappedConnection.createStatement(), config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(String sql) throws SQLException {
        return new LoggablePreparedStatement(wrappedConnection.prepareStatement(sql), sql, config);
    }

    @Override
    public LoggableCallableStatement prepareCall(String sql) throws SQLException {
        return new LoggableCallableStatement(wrappedConnection.prepareCall(sql), sql, config);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return wrappedConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrappedConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return wrappedConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        wrappedConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        wrappedConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
        if (config.closeResourcesQuietly) {
            Queryable.closeQuietly(wrappedConnection);
        } else {
            wrappedConnection.close();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return wrappedConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        wrappedConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        wrappedConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        wrappedConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrappedConnection.clearWarnings();
    }

    @Override
    public LoggableStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new LoggableStatement(wrappedConnection.createStatement(resultSetType, resultSetConcurrency), config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new LoggablePreparedStatement(
                wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, config);
    }

    @Override
    public LoggableCallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new LoggableCallableStatement(
                wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency), sql, config);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrappedConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrappedConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        wrappedConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return wrappedConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return wrappedConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return wrappedConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        wrappedConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrappedConnection.releaseSavepoint(savepoint);
    }

    @Override
    public LoggableStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new LoggableStatement(
                wrappedConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new LoggablePreparedStatement(
                wrappedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, config);
    }

    @Override
    public LoggableCallableStatement prepareCall(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        CallableStatement stmt =
                wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new LoggableCallableStatement(stmt, sql, config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new LoggablePreparedStatement(wrappedConnection.prepareStatement(sql, autoGeneratedKeys), sql, config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new LoggablePreparedStatement(wrappedConnection.prepareStatement(sql, columnIndexes), sql, config);
    }

    @Override
    public LoggablePreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new LoggablePreparedStatement(wrappedConnection.prepareStatement(sql, columnNames), sql, config);
    }

    @Override
    public Clob createClob() throws SQLException {
        return wrappedConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return wrappedConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return wrappedConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return wrappedConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return wrappedConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrappedConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrappedConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return wrappedConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return wrappedConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrappedConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrappedConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        wrappedConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return wrappedConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrappedConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrappedConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrappedConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrappedConnection.isWrapperFor(iface);
    }
}
