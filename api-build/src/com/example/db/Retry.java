package com.example.db;

import com.example.logger.ExampleLogger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

///** TODO replace usages with {@link com.example.lambda.Retrier}. See #62418 */
@Deprecated
@SuppressWarnings({"java:S1444", "java:S1104"})
public final class Retry {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    static final int MAX_ATTEMPTS = 4;

    // expose to speed up testing
    public static Function<Integer, Duration> sleepWaitDuration =
            attempt -> Duration.ofMinutes(Math.round(Math.pow(2, attempt)));

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    public interface FinalExceptionThrower<Ex1 extends Exception, Ex2 extends Exception> {
        void accept(Exception e) throws Ex1, Ex2;
    }

    /**
     * Defines the retry logic used throughout {@link Retry}.
     *
     * @param callable - {@link Callable#call()} contains code you wish to retry in the event of an exception
     * @param nonRetryableExceptions - A list of exceptions that, when caught, will bypass the retry logic and cause
     *     immediate failure.
     * @param finalExceptionThrower - Allows custom exceptions to be thrown when {@link Retry#MAX_ATTEMPTS} is
     *     surpassed. If this function throws nothing, a {@link RuntimeException} is thrown by default.
     * @param <T> - The type of the value returned by the callable.
     * @param <Ex1> - The first type of the exception potentially thrown by the callable. {@link RuntimeException} is
     *     the default value.
     * @param <Ex2> - The second type of the exception potentially thrown by the callable. {@link RuntimeException} is
     *     the default value.
     * @return The value returned by the callable. Can be {@link Void}
     * @throws Ex1
     * @throws Ex2
     */
    static <T, Ex1 extends Exception, Ex2 extends Exception> T actRoot(
            Callable<T> callable,
            List<Class> nonRetryableExceptions,
            FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower)
            throws Ex1, Ex2 {

        String callerSummary =
                Arrays.stream(Thread.currentThread().getStackTrace())
                        .filter(n -> !n.getClassName().equals(Thread.class.getName()))
                        .filter(n -> !n.getClassName().equals(Retry.class.getName()))
                        .filter(n -> !n.getClassName().equals(com.example.db.SqlDbSource.class.getName()))
                        .map(ci -> ci.getClassName() + "#" + ci.getMethodName() + ":" + ci.getLineNumber())
                        .findFirst()
                        .orElse("unknown caller");

        for (int attempt = 1; /* see catch */ ; attempt++) {
            try {
                return callable.call();
            } catch (Exception e) {
                e.printStackTrace();
//                if (e instanceof CompleteWithTask) throw (CompleteWithTask) e;
//                if (e instanceof Rescheduled) throw (Rescheduled) e;
//
//                if (attempt >= MAX_ATTEMPTS) {
//                    finalExceptionThrower.accept(e);
//                    // throw a RuntimeException here in case finalExceptionThrower is ineffective
//                    throw new RuntimeException(e);
//                }
//
//                Throwable curr = e;
//                while (curr != null) {
//                    if (nonRetryableExceptions.contains(curr.getClass())) {
//                        throw new RuntimeException(curr);
//                    } else if (curr instanceof MaxDiskUsageException) {
//                        throw (MaxDiskUsageException) curr;
//                    }
//                    curr = curr.getCause();
//                }

                sleepWithExponentialBackoff(attempt, e, callerSummary);
            }
        }
    }

    public static <T> T act(Callable<T> callable) {
        return actRoot(callable, Collections.emptyList(), __ -> {});
    }

    public static <T> T act(Callable<T> callable, List<Class> nonRetryableExceptions) {
        return actRoot(callable, nonRetryableExceptions, __ -> {});
    }

    public static <T, Ex1 extends Exception, Ex2 extends Exception> T act(
            Callable<T> callable, FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower) throws Ex1, Ex2 {

        return actRoot(callable, Collections.emptyList(), finalExceptionThrower);
    }

    public static <T, Ex1 extends Exception, Ex2 extends Exception> T act(
            Callable<T> callable,
            List<Class> nonRetryableExceptions,
            FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower)
            throws Ex1, Ex2 {

        return actRoot(callable, nonRetryableExceptions, finalExceptionThrower);
    }

    public static void act(CheckedRunnable checkedRunnable) {

        act(checkedRunnable, Collections.emptyList(), __ -> {});
    }

    public static void act(CheckedRunnable checkedRunnable, List<Class> nonRetryableExceptions) {

        act(checkedRunnable, nonRetryableExceptions, __ -> {});
    }

    public static <Ex1 extends Exception, Ex2 extends Exception> void act(
            CheckedRunnable checkedRunnable, FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower) throws Ex1, Ex2 {

        act(checkedRunnable, Collections.emptyList(), finalExceptionThrower);
    }

    public static <Ex1 extends Exception, Ex2 extends Exception> void act(
            CheckedRunnable checkedRunnable,
            List<Class> nonRetryableExceptions,
            FinalExceptionThrower<Ex1, Ex2> finalExceptionThrower)
            throws Ex1, Ex2 {

        Callable<Void> callableWrapper =
                () -> {
                    checkedRunnable.run();
                    return null;
                };

        actRoot(callableWrapper, nonRetryableExceptions, finalExceptionThrower);
    }

    static void sleepWithExponentialBackoff(int attempt, Throwable t, String callerSummary) {
        try {
            // subtract by one to get desired exponential sequence (0, 1, 2)
            Duration waitTime = sleepWaitDuration.apply(attempt - 1);
            String warning =
                    "Exception at "
                            + callerSummary
                            + " after attempt "
                            + attempt
                            + " -- waiting "
                            + waitTime.getSeconds()
                            + " seconds before retrying";

//            LOG.log(Level.WARNING, warning, t);

            Thread.sleep(waitTime.toMillis());
        } catch (InterruptedException e) {
            // See https://sonarqube.it-example.com/coding_rules?open=java%3AS2142&rule_key=java%3AS2142
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
