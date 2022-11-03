package com.example.sql_server;

import java.util.function.Predicate;

public class LoggableConfig {

    public final boolean writesToCustomerLogs;
    public final boolean connectionQueryNumberMappingLogs;
    public final boolean closeResourcesQuietly;
    public final boolean logQueryExecutionTime;
    public final boolean logFetchMoreExecutionTime;
    public final Predicate<Long> fetchMoreLogIfPredicate;

    protected LoggableConfig(
            boolean writesToCustomerLogs,
            boolean connectionQueryNumberMappingLogs,
            boolean closeResourcesQuietly,
            boolean logQueryExecutionTime,
            boolean logFetchMoreExecutionTime,
            Predicate<Long> fetchMoreLogIfPredicate) {

        this.writesToCustomerLogs = writesToCustomerLogs;
        this.connectionQueryNumberMappingLogs = connectionQueryNumberMappingLogs;
        this.closeResourcesQuietly = closeResourcesQuietly;
        this.logQueryExecutionTime = logQueryExecutionTime;
        this.logFetchMoreExecutionTime = logFetchMoreExecutionTime;
        this.fetchMoreLogIfPredicate = fetchMoreLogIfPredicate;
    }

    public static LoggableConfig defaultConfig() {
        return new Builder().build();
    }

    public static class Builder {

        private boolean writesToCustomerLogs = false;
        private boolean connectionQueryNumberMappingLogs = false;
        private boolean closeResourcesQuietly = false;
        private boolean logQueryExecutionTime = true;
        private boolean logFetchMoreExecutionTime = true;
        private Predicate<Long> fetchMoreLogIfPredicate = milliseconds -> milliseconds > 500;

        public Builder() {}

        /**
         * set if the log output should also go to the customer logs
         *
         * <p>Default: false
         */
        public Builder setWritesToCustomerLogs(boolean writesToCustomerLogs) {
            this.writesToCustomerLogs = writesToCustomerLogs;
            return this;
        }

        /**
         * set if the connection object to query number mapping should be logged
         *
         * <p>Default: false
         */
        public Builder setConnectionQueryNumberMappingLog(boolean connectionQueryNumberMappingLogs) {
            this.connectionQueryNumberMappingLogs = connectionQueryNumberMappingLogs;
            return this;
        }

        /**
         * set if closing the resources created will ignore errors or not
         *
         * <p>Default: false
         */
        public Builder setCloseResourcesQuietly(boolean closeResourcesQuietly) {
            this.closeResourcesQuietly = closeResourcesQuietly;
            return this;
        }

        /**
         * set if the time a query takes to execute should be logged
         *
         * <p>Default: true
         */
        public Builder setLogQueryExecutionTime(boolean logQueryExecutionTime) {
            this.logQueryExecutionTime = logQueryExecutionTime;
            return this;
        }

        /**
         * set if the time a result set spends fetching more rows from the database should be logged
         *
         * <p>Default: true
         */
        public Builder setLogFetchMoreExecutionTime(boolean logFetchMoreExecutionTime) {
            this.logFetchMoreExecutionTime = logFetchMoreExecutionTime;
            return this;
        }

        /**
         * Set a predicate for checking the minimum time that must have elapsed to log the time to fetch more rows.
         *
         * <p>Default: time > 500ms
         *
         * <p>The logFetchMoreExecutionTime logging works by logging the time LoggableResultSet.next() takes to run. The
         * problem is that .next() will usually NOT perform a network operation. If the time in ms that .next() takes to
         * return passes this Predicate, we will log the time it took. The default of 500ms is long enough that we
         * should never catch a non-network .next() call. There _might_ be network requests faster than that, but in
         * that case the connector needs to be tweaked if the logging is desired. Usually this logging is useful when
         * network requests take up a majority of the process time.
         */
        public Builder setFetchMoreLogIfPredicate(Predicate<Long> fetchMoreLogIfPredicate) {
            this.fetchMoreLogIfPredicate = fetchMoreLogIfPredicate;
            return this;
        }

        public LoggableConfig build() {
            return new LoggableConfig(
                    writesToCustomerLogs,
                    connectionQueryNumberMappingLogs,
                    closeResourcesQuietly,
                    logQueryExecutionTime,
                    logFetchMoreExecutionTime,
                    fetchMoreLogIfPredicate);
        }
    }
}