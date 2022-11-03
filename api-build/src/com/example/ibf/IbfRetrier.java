package com.example.ibf;

import com.example.db.Retrier;
import com.example.logger.ExampleLogger;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.logging.Level;

public class IbfRetrier extends Retrier {

    private final List<Class<? extends Exception>> expectedRetryExceptions;

    public IbfRetrier(List<Class<? extends Exception>> expectedRetryExceptions) {
        this.expectedRetryExceptions = expectedRetryExceptions;
    }

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    // expose for test
    public static Function<Integer, Duration> sleepWaitDuration =
            attempt -> Duration.ofMinutes(Math.round(Math.pow(2, attempt)));

    @Override
    protected Exception handleException(Exception exception, int attempt) throws Exception {
        if (isExpectedException(exception.getClass())) {
            return exception;
        }
        throw exception;
    }

    @Override
    protected void waitForRetry(Exception exception, int attempt) throws InterruptedException {
        Duration waitTime = sleepWaitDuration.apply(attempt - 1);

        String warning =
                "Exception at "
                        + exception.getMessage()
                        + " after attempt "
                        + attempt
                        + " -- waiting "
                        + waitTime.getSeconds()
                        + " seconds before retrying";
        LOG.log(Level.WARNING, warning, exception);
        try {
            Thread.sleep(waitTime.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSource get(Callable<DataSource> dataSourceInit, int i) {
        return null;
    }

    private boolean isExpectedException(Class<? extends Exception> exceptionType) {
        for (Class<? extends Exception> expectedExceptionType : expectedRetryExceptions) {
            if (expectedExceptionType.equals(exceptionType)) {
                return true;
            }
        }
        return false;
    }
}
