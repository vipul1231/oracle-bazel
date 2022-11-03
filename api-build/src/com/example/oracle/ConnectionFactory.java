package com.example.oracle;

import com.example.micrometer.MetricDefinition;
import com.example.oracle.exceptions.MissingObjectException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.function.Function;

import static com.example.oracle.Constants.MAX_RETRIES;
import static com.example.oracle.OracleErrorCode.ORA_06564;
import static com.example.oracle.Transactions.sleepDurationFunction;
import static com.example.oracle.Transactions.sleepForRetry;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/9/2021<br/>
 * Time: 12:01 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionFactory implements AutoCloseable {
    private static ConnectionFactory connectionFactory = null;
    /**
     * Connection to the database, possibly via an SSH tunnel and the port forwarder
     */
    private static DataSource dataSource;

    private ConnectionFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static ConnectionFactory getInstance() {
        if (connectionFactory == null) {
            throw new AssertionError("You have to call init first");
        }

        return connectionFactory;
    }

    public synchronized static ConnectionFactory init(DataSource dataSource) {
        connectionFactory = new ConnectionFactory(dataSource);
        return connectionFactory;
    }

    @Override
    public void close() {
        try {
            if (dataSource instanceof AutoCloseable) ((AutoCloseable) dataSource).close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Connection connectToSourceDb() {
        for (int attempt = 1; /* see catch */ ; attempt++) {
            try {
                Connection connection = dataSource.getConnection();

                // Auto-commit must be disabled because otherwise the whole result set will be fetched for each table.
                // setFetchSize will not take effect without disabling auto-commit
                connection.setAutoCommit(false);

                return connection;

            } catch (SQLException e) {
                if (attempt >= MAX_RETRIES) {
//                    throw CompleteWithTask.reconnect(e);
                }
                sleepForRetry(Transactions.quickSleepDurationFunction, attempt, e, "connectToSourceDb");
            }
        }
    }


    <ReturnType> ReturnType retry(String operand, Transactions.RetryFunction<ReturnType> action) {
        Function<Exception, RuntimeException> finalExceptionWrapper =
                t -> new RuntimeException("Failed to perform " + operand + " after " + MAX_RETRIES + " attempts", t);

        return retry(operand, finalExceptionWrapper, action);
    }

    public <ReturnType, ExceptionType extends Throwable> ReturnType retry(
            String operand,
            Function<Exception, ExceptionType> finalExceptionWrapper,
            Transactions.RetryFunctionThrowable<ReturnType, ExceptionType> action)
            throws ExceptionType {
        return retryWithFailureCounter(operand, finalExceptionWrapper, action, null);
    }

    <ReturnType, ExceptionType extends Throwable> ReturnType retryWithFailureCounter(
            String operand,
            Function<Exception, ExceptionType> finalExceptionWrapper,
            Transactions.RetryFunctionThrowable<ReturnType, ExceptionType> action,
            MetricDefinition failureCountMetric)
            throws ExceptionType {
        for (int attempt = 1; /* see catch */ ; attempt++) {
            try (Connection connection = connectToSourceDb()) {
                return action.accept(connection);
            } catch (SQLDataException e) {
                // these are non-recoverable exceptions. Usually related to bad data in a query like dividing by zero
                // see: https://docs.oracle.com/javase/8/docs/api/index.html?java/sql/SQLDataException.html
                throw finalExceptionWrapper.apply(e);
            } catch (Exception
                    e) { // kluge for ArrayIndexOutOfBoundsException from the OJDBC driver (#64408)
                if (ORA_06564.is(e.getMessage())) { // ORA-06564: object name does not exist
                    throw new MissingObjectException(e.getMessage());
                }

                if (attempt >= MAX_RETRIES) {
                    throw finalExceptionWrapper.apply(e);
                }

                sleepForRetry(sleepDurationFunction, attempt, e, operand);
                if (e instanceof ArrayIndexOutOfBoundsException)
                    attempt = MAX_RETRIES; // retry only once for for ArrayIndexOutOfBoundsException.
            } finally {
                int failures = attempt - 1;
                if (failureCountMetric != null && failures > 0) {
                    // TODO
//                    OracleMetricsHelper.publishFailureCountMetric(failureCountMetric, failures);
                }
            }
        }
    }
}