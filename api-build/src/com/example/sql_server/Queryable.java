package com.example.sql_server;


import com.example.logger.ExampleLogger;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Matcher;

public abstract class Queryable {

    private static final AtomicLong GLOBAL_QUERY_NUMBER = new AtomicLong(0);

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    final ArrayList<Object> parameterValues = new ArrayList<>();
    final ArrayList<String> batchedSqlQueries = new ArrayList<>();

    final LoggableConfig config;
    final long queryNumber;

    Queryable(LoggableConfig config) {
        this(config, GLOBAL_QUERY_NUMBER.getAndIncrement());
    }

    Queryable(LoggableConfig config, long queryNumber) {
        this.config = config;
        this.queryNumber = queryNumber;
    }

    void saveQueryIndexedParam(int position, Object obj) {
        String strValue = convertObjectToString(obj);

        // if we are setting a position larger than current size of parameterValues, first make it larger
        while (position >= parameterValues.size()) parameterValues.add(null);

        // save the parameter
        parameterValues.set(position, strValue);
    }

    String convertObjectToString(Object obj) {
        if (Objects.isNull(obj)) return "null";

        if (obj instanceof String || obj instanceof Date) return "'" + obj + "'";

        return obj.toString();
    }

    void logQuery(String rawSql) {
        String processedSql = processQuery(rawSql);
        System.out.println(processedSql);
//        SqlQueryEvent event = SqlQueryEvent.of(processedSql, queryNumber);

//        if (config.writesToCustomerLogs) LOG.customerInfo(event);
//        else LOG.info(event);

        if (config.connectionQueryNumberMappingLogs)
            wrappedStatement().ifPresent(v -> LOG.info(v + " is mapped to QueryNumber : " + queryNumber));
    }

    /** Each loggable* uses wrappedStatement, so we are fetching it here for connection to query number log */
    protected Optional<String> wrappedStatement() {
        return Optional.empty();
    }

    void addQueryToBatch(String rawSql) {
        batchedSqlQueries.add(processQuery(rawSql));
        parameterValues.clear();
    }

    void logBatchedQueries() {
        batchedSqlQueries.forEach(
                query -> {
//                    SqlQueryEvent event = SqlQueryEvent.of(query, queryNumber);
//
//                    if (config.writesToCustomerLogs) LOG.customerInfo(event);
//                    else LOG.info(event);
                });
        batchedSqlQueries.clear();
    }

    void clearBatchedQueries() {
        batchedSqlQueries.clear();
    }

    String processQuery(String rawSql) {
        return addIndexedParamsToSql(rawSql);
    }

    String addIndexedParamsToSql(String sql) {
        // Replace `?` with values for PreparedStatement
        // index starts from `1` because prepared statement index set starts from `1`
        for (int index = 1; index < parameterValues.size(); index++) {
            String value = parameterValues.get(index) == null ? "null" : parameterValues.get(index).toString();
            sql = sql.replaceFirst("\\?", Matcher.quoteReplacement(value));
        }
        return sql;
    }

    interface SqlCallable<V> {
        V call() throws SQLException;
    }

    <ReturnType> ReturnType time(SqlCallable<ReturnType> callable, String descriptor) throws SQLException {
        return timeLogIf(callable, descriptor, __ -> true);
    }

    <ReturnType> ReturnType timeQuery(SqlCallable<ReturnType> callable) throws SQLException {
        if (config.logQueryExecutionTime) {
            return time(callable, "Query Execution");
        } else {
            return callable.call();
        }
    }

    <ReturnType> ReturnType timeLogIf(
            SqlCallable<ReturnType> callable, String descriptor, Predicate<Long> logIfCondition) throws SQLException {

        long start = System.nanoTime();
        ReturnType returnValue = callable.call();
        long totalMs = (System.nanoTime() - start) / 1000000;

        if (logIfCondition.test(totalMs)) {
            LOG.info("Query Number : " + queryNumber + ": " + descriptor + " took " + totalMs + "ms");
        }

        return returnValue;
    }

    public static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception e) {
//            LOG.log(
//                    Level.WARNING,
//                    "Failed closing " + closeable.getClass().getCanonicalName() + " (non-fatal): " + e.getMessage(),
//                    e);
        }
    }
}
