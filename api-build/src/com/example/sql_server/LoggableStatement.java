package com.example.sql_server;

import java.sql.*;
import java.util.Optional;

public class LoggableStatement extends Queryable implements Statement {
    private final Statement wrappedStatement;

    LoggableStatement(Statement wrappedStatement) {
        this(wrappedStatement, LoggableConfig.defaultConfig());
    }

    public LoggableStatement(Statement wrappedStatement, LoggableConfig config) {
        super(config);
        this.wrappedStatement = wrappedStatement;
    }

    @Override
    protected Optional<String> wrappedStatement() {
        return Optional.of(wrappedStatement.toString());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logQuery(sql);
        ResultSet rs = timeQuery(() -> wrappedStatement.executeQuery(sql));
        return new LoggableResultSet(rs, config, queryNumber);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.executeUpdate(sql));
    }

    @Override
    public void close() throws SQLException {
        if (config.closeResourcesQuietly) {
            Queryable.closeQuietly(wrappedStatement);
        } else {
            wrappedStatement.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return wrappedStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        wrappedStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return wrappedStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        wrappedStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        wrappedStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return wrappedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        wrappedStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        wrappedStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrappedStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        wrappedStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.execute(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return new LoggableResultSet(wrappedStatement.getResultSet(), config, queryNumber);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return wrappedStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return wrappedStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        wrappedStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return wrappedStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        wrappedStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return wrappedStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return wrappedStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return wrappedStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        addQueryToBatch(sql);
        wrappedStatement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        clearBatchedQueries();
        wrappedStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        logBatchedQueries();
        return timeQuery(wrappedStatement::executeBatch);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return wrappedStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return wrappedStatement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new LoggableResultSet(wrappedStatement.getGeneratedKeys(), config, queryNumber);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.executeUpdate(sql, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.executeUpdate(sql, columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.executeUpdate(sql, columnNames));
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.execute(sql, autoGeneratedKeys));
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.execute(sql, columnIndexes));
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedStatement.execute(sql, columnNames));
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return wrappedStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        wrappedStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return wrappedStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        wrappedStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return wrappedStatement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrappedStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrappedStatement.isWrapperFor(iface);
    }
}
