package com.example.oracle.util;

import com.example.core.CompleteWithTask;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.micrometer.MetricDefinition;
import com.example.oracle.Container;
import com.example.oracle.OracleErrorCode;
import com.example.oracle.OracleMetricsHelper;
import com.example.oracle.Transactions;
import com.example.oracle.exceptions.MissingObjectException;
import com.example.oracle.system.DatabaseValidator;
import java.sql.*;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import javax.sql.DataSource;

public class ConnectionManagerWithRetry implements AutoCloseable {

    private static final int MAX_RETRIES = 3;
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    private static final OracleErrorCode ORA_06564 = OracleErrorCode.ORA_06564;
    Function<Integer, Duration> sleepDurationFunction = attempt -> Duration.ofMinutes(Math.round(Math.pow(2, attempt)));
    Function<Integer, Duration> quickSleepDurationFunction =
            attempt -> Duration.ofSeconds(Math.round(Math.pow(3, attempt)));
    /** Connection to the database, possibly via an SSH tunnel and the port forwarder */
    private final DataSource dataSource;

    private DatabaseValidator databaseValidator = new DatabaseValidator();
    private final String pdbName;
    private final boolean isContainerizedDB;
    private static final String CDB_ROOT = "CDB$ROOT";

    public ConnectionManagerWithRetry(DataSource dataSource, Optional<String> pdbName) {
        this.dataSource = dataSource;

        if (pdbName.isPresent()) {
            try (Connection connection = connectToSourceDb(Container.CDB)) {
                isContainerizedDB = isContainerizedDB(connection);
                this.pdbName = pdbName.get();
                LOG.info("pdbName is:" + pdbName);
            } catch (SQLException e) {
                LOG.warning("Exception while checking if DB is Multitenant");
                throw new RuntimeException("Exception while checking if DB is Multitenant", e);
            }
        } else {
            isContainerizedDB = false;
            this.pdbName = null;
        }
    }

    public boolean isContainerizedDB(Connection connection) {
        try {
            return databaseValidator.isContainerizedDB(connection);
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("java:S2095")
    public Connection connectToSourceDb(Container container) throws SQLException {
        for (int attempt = 1; /* see catch */ ; attempt++) {
            try {
                Connection connection = dataSource.getConnection();

                // Auto-commit must be disabled because otherwise the whole result set will be fetched for each table.
                // setFetchSize will not take effect without disabling auto-commit
                connection.setAutoCommit(false);

                if (isContainerizedDB) {
                    alterConnectionSession(connection, container);
                }
                return connection;

            } catch (SQLException e) {
                if (attempt >= MAX_RETRIES) {
                    throw e;
                }
                sleepForRetry(quickSleepDurationFunction, attempt, e, "connectToSourceDb");
            }
        }
    }

    public void alterConnectionSession(Connection connection, Container container) {
        String containerId = container == Container.PDB ? pdbName : CDB_ROOT;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER SESSION SET CONTAINER = " + containerId);
        } catch (SQLException e) {
            throw new RuntimeException("Exception while changing session to container " + containerId, e);
        }
    }

    public void sleepForRetry(
            Function<Integer, Duration> durationFunction, int attempt, Throwable t, String callerSummary) {

        sleepForRetry(durationFunction.apply(attempt), attempt, t, callerSummary);
    }

    public void sleepForRetry(Duration waitTime, int attempt, Throwable t, String callerSummary) {

        try {
            String warning =
                    "Exception at "
                            + callerSummary
                            + " after attempt "
                            + attempt
                            + " -- waiting "
                            + waitTime.getSeconds()
                            + " seconds before retrying";

            LOG.log(Level.WARNING, warning, t);

            Thread.sleep(waitTime.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public <ReturnType> ReturnType retry(String operand, Container container, Transactions.RetryFunction<ReturnType> action) throws Exception {
        Function<Exception, RuntimeException> finalExceptionWrapper =
                t -> new RuntimeException("Failed to perform " + operand + " after " + MAX_RETRIES + " attempts", t);

        return retry(operand, finalExceptionWrapper, container, action);
    }

    public <ReturnType, ExceptionType extends Throwable> ReturnType retry(
            String operand,
            Function<Exception, ExceptionType> finalExceptionWrapper,
            Container container,
            Transactions.RetryFunctionThrowable<ReturnType, ExceptionType> action)
            throws ExceptionType, Exception {
        return retryWithFailureCounter(operand, finalExceptionWrapper, container, action, null);
    }

    public <ReturnType, ExceptionType extends Throwable> ReturnType retryWithFailureCounter(
            String operand,
            Function<Exception, ExceptionType> finalExceptionWrapper,
            Container container,
            Transactions.RetryFunctionThrowable<ReturnType, ExceptionType> action,
            MetricDefinition failureCountMetric)
            throws ExceptionType, Exception {
        for (int attempt = 1; /* see catch */ ; attempt++) {
            try (Connection connection = connectToSourceDb(container)) {
                return action.accept(connection);
            } catch (SQLDataException e) {
                // these are non-recoverable exceptions. Usually related to bad data in a query like dividing by zero
                // see: https://docs.oracle.com/javase/8/docs/api/index.html?java/sql/SQLDataException.html
                throw finalExceptionWrapper.apply(e);
            } catch (SQLException
                    | ArrayIndexOutOfBoundsException
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
                }
            }
        }
    }

    public void setSleepDurationFunction(Function<Integer, Duration> sleepDurationFunction) {
        this.sleepDurationFunction = sleepDurationFunction;
    }

    @Override
    public void close() throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }
}
