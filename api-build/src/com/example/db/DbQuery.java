package com.example.db;

import com.example.logger.ExampleLogger;

import java.sql.*;
import java.util.Objects;

/** A wrapper for JDBC statements to provide some common error handling */
public class DbQuery implements AutoCloseable {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private Statement statement;

    public DbQuery(Statement statement) {
        this.statement = Objects.requireNonNull(statement);
    }

    public ResultSet executeQuery(String query) throws SQLException {
        try {
            return statement.executeQuery(query);
        } catch (SQLRecoverableException e) {
            cancelQueryIfTimeout(e);

            throw e;
        }
    }

    /**
     * Calls PreparedStatement.executeQuery() but catches the Socket read timed out exception and attempts to cancel the
     * query. If an exception occurs during cancellation it is logged as a warning but the original exception is thrown.
     *
     * @return
     * @throws SQLException
     */
    public ResultSet executeQuery() throws SQLException {

        if (!PreparedStatement.class.isAssignableFrom(statement.getClass())) {
            throw new IllegalStateException(
                    "Statement is not a PreparedStatement: the executeQuery method is not available on this statement type.");
        }

        try {
            return ((PreparedStatement) statement).executeQuery();
        } catch (SQLRecoverableException e) {
            cancelQueryIfTimeout(e);

            throw e;
        }
    }

    /**
     * In some cases when a socket read times out the query on the DB may still be executing and using DB resources.
     * This method safely attempts to cancel the query. Any exceptions encountered while calling cancel are ignored.
     *
     * @param e
     */
    private void cancelQueryIfTimeout(SQLRecoverableException e) {
        if (e.getMessage().contains("Socket read timed out")) {
            try {
                if (!statement.getConnection().isClosed()) {
                    statement.cancel();
                    LOG.info("Query cancelled.");
                }
            } catch (Exception ignore) {
                // Don't let this exception overwrite the original exception, we don't care if
                // cancel succeeded or not.
                // In many cases this will just be a SQLRecoverableException reporting
                // that the connection has been closed.
            }
        }
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }
}
