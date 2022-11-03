package com.example.db;

import com.amazonaws.util.Throwables;
import com.example.core.DbCredentials;
import com.example.snowflake.util.DBUtility;
import com.example.core2.ConnectionParameters;

import com.example.sql_server.LoggableDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public abstract class SqlDbSource<SqlCreds extends DbCredentials> implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(DBUtility.class.getName());
    public SqlCreds originalCredentials;
    public ConnectionParameters params;
    protected TunnelableConnection tunnelableConnection;
    public DbCredentials credentials;
    public com.example.sql_server.LoggableDataSource dataSource;

    @FunctionalInterface
    public interface Query<T> {
        T execute(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface Statement {
        void execute(Connection connection) throws Exception;
    }

    public SqlDbSource() {
    }

    public SqlDbSource(SqlCreds originalCredentials, ConnectionParameters params) {
        this.originalCredentials = originalCredentials;
        this.params = params;
        this.tunnelableConnection = new TunnelableConnection(originalCredentials, params, false);
        this.credentials =
                new DbCredentials(
                        this.tunnelableConnection.host,
                        this.tunnelableConnection.port,
                        originalCredentials.database,
                        originalCredentials.user,
                        originalCredentials.password
                );
        this.dataSource =
                new LoggableDataSource(nonLoggableDataSource(originalCredentials, credentials, params), true);
    }

    public SqlDbSource(SqlCreds originalCredentials, ConnectionParameters params, DataSource dataSource) {
        try {
            this.originalCredentials = originalCredentials;
            this.params = params;
            this.tunnelableConnection = new TunnelableConnection(originalCredentials, params, false);
            if (dataSource != null) {
                this.dataSource = new LoggableDataSource(dataSource);
            } else {
                this.credentials =
                        new DbCredentials(
                                this.tunnelableConnection.host,
                                this.tunnelableConnection.port,
                                originalCredentials.database,
                                originalCredentials.user,
                                originalCredentials.password
                        );
                this.dataSource =
                        new LoggableDataSource(nonLoggableDataSource(originalCredentials, credentials, params), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            throw CompleteWithTask.reconnect(e);
        }
    }

    public SqlCreds getOriginalCredentials() {
        return originalCredentials;
    }

    protected abstract DataSource nonLoggableDataSource(
            SqlCreds originalCredentials, DbCredentials credentials, ConnectionParameters params);

    protected boolean allowPlainText() {
//        return tunnelableConnection.tunnel.isPresent() && (!credentials.alwaysEncrypted);
        return false;
    }

    /**
     * Non-final for testing purposes -- not intended for extension.
     *
     * @return Connection -- WARNING: this resource must be closed manually or with a try-with-resources. Use {@link
     * SqlDbSource#executeWithRetry(Query)} when possible.
     * @throws SQLException
     */
    public Connection connection() throws SQLException {
        return dataSource.getConnection();
    }

    public DataSource dataSource() {
        return dataSource;
    }

    /**
     * Returns an ineffective {@link Retry.FinalExceptionThrower} by default. Override if your {@link SqlDbSource} has
     * general exception handling you wish to apply to any invocation of {@link SqlDbSource#executeWithRetry}.
     *
     * <p>We reserve the exception type parameters for the actual caller of {@link SqlDbSource#executeWithRetry}.
     * Therefore, this method should either throw a RuntimeException or do nothing.
     */
    protected Retry.FinalExceptionThrower<RuntimeException, RuntimeException> baseFinalExceptionThrower() {
        return __ -> {
        };
    }

    public final <T, Ex1 extends Exception, Ex2 extends Exception> T executeWithRetry(
            Query<T> query,
            List<Class> nonRetryableExceptions,
            Retry.FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower)
            throws Ex1, Ex2 {

        Retry.FinalExceptionThrower<Ex1, Ex2> finalExceptionThrowerWithBase =
                e -> {
                    finalExceptionThrower.accept(e);
                    baseFinalExceptionThrower().accept(e);
                };

        Callable<T> callable =
                () -> {
                    try (Connection connection = connection()) {
                        return query.execute(connection);
                    }
                };

        return Retry.act(callable, nonRetryableExceptions, finalExceptionThrowerWithBase);
    }

    public final <T, Ex1 extends Exception, Ex2 extends Exception> T executeWithRetry(
            Query<T> query, Retry.FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower) throws Ex1, Ex2 {

        return executeWithRetry(query, Collections.emptyList(), finalExceptionThrower);
    }

    public final <T> T executeWithRetry(Query<T> query, List<Class> nonRetryableExceptions) {

        return executeWithRetry(query, nonRetryableExceptions, __ -> {
        });
    }

    public final <T> T executeWithRetry(Query<T> query) {
        return executeWithRetry(query, Collections.emptyList(), __ -> {
        });
    }

    public final <Ex1 extends Exception, Ex2 extends Exception> void executeWithRetry(
            Statement statement,
            List<Class> nonRetryableExceptions,
            Retry.FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower)
            throws Ex1, Ex2 {

        Query<Void> wrappedStatement =
                connection -> {
                    statement.execute(connection);
                    return null;
                };

        executeWithRetry(wrappedStatement, nonRetryableExceptions, finalExceptionThrower);
    }

    public <Ex1 extends Exception, Ex2 extends Exception> void executeWithRetry(
            Statement statement, Retry.FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower) throws Ex1, Ex2 {

        executeWithRetry(statement, Collections.emptyList(), finalExceptionThrower);
    }

    public final void executeWithRetry(Statement statement, List<Class> nonRetryableExceptions) {

        executeWithRetry(statement, nonRetryableExceptions, __ -> {
        });
    }

    public final void executeWithRetry(Statement statement) {
        executeWithRetry(statement, Collections.emptyList(), __ -> {
        });
    }

    @Override
    public void close() {
        if (dataSource != null) dataSource.close();
    }

    public final void execute(Statement statement) {
        execute(statement, null);
    }

    public final <Ex extends Exception> void execute(
            Statement statement, Exception finalExceptionThrower) throws Ex {
        Query<Void> wrappedStatement =
                connection -> {
                    statement.execute(connection);
                    return null;
                };

        execute(wrappedStatement, finalExceptionThrower);
    }

    public final <T> T execute(Query<T> query) {
        return execute(query, null);
    }

    public final <T, Ex extends Exception> T execute(
            Query<T> query, Exception finalExceptionThrower) throws Ex {

        try (Connection connection = connection()) {
            return query.execute(connection);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Got an unexpected interrupt", ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            if (finalExceptionThrower != null) System.out.println(ex.getMessage());

            // If nothing happens then throw a RuntimeException
            throw new RuntimeException(ex);
        }
    }

    public void consume(SQLConsumer<java.sql.Statement> statementConsumer) {
        try (Connection connection = connection(); java.sql.Statement statement = connection.createStatement();) {
            statementConsumer.accept(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String update(String format, Object...args) {
        String sql = String.format(format, args);
        logger.info("Update: " + sql);
        try (Connection connection = connection(); java.sql.Statement statement = connection.createStatement();) {
            statement.executeUpdate(sql);
            return "";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class DataSourceInitRetrier extends Retrier {

        private static final Duration WAIT_FOR_RETRY = Duration.ofSeconds(1);
        private final Collection<Class<? extends Throwable>> ignoredExceptionList;

        public DataSourceInitRetrier() {
            ignoredExceptionList = new ArrayList<>();
            ignoredExceptionList.add(TimeoutException.class);
        }

        public DataSourceInitRetrier(Class<? extends Throwable>... ignoredExceptionList) {
            this.ignoredExceptionList = Arrays.asList(ignoredExceptionList);
        }

        /**
         * Retry on {@link TimeoutException}s thrown by connection timeouts in {@link
         * InterruptibleMysqlDataSource}
         *
         * <p>{@inheritDoc}
         */
        @Override
        protected Exception handleException(Exception exception, int attempt) throws Exception {
            if (ignoredExceptionList.contains(Throwables.getRootCause(exception).getClass())) {
                String warning =
                        "Exception after attempt "
                                + attempt
                                + " -- waiting "
                                + WAIT_FOR_RETRY.getSeconds()
                                + " seconds before retrying";

                //LOG.log(Level.WARNING, warning, exception);

                return exception;
            }
            throw exception;
        }

        /**
         * Because DataSource initialization occurs frequently in the front end, we rapidly retry initialization
         * attempts.
         *
         * <p>{@inheritDoc}
         */
        @Override
        protected void waitForRetry(Exception exception, int attempt) throws InterruptedException {
            Thread.sleep(WAIT_FOR_RETRY.toMillis());
        }

        @Override
        public DataSource get(Callable<DataSource> dataSourceInit, int i) {
            try {
                return dataSourceInit.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @FunctionalInterface
    public interface SQLConsumer<T> {
        void accept(T input) throws SQLException;
    }

    @FunctionalInterface
    public interface SQLFunction<T,F> {
        F apply(T input) throws SQLException;
    }
}
