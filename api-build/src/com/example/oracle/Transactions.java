package com.example.oracle;

import com.example.core.TableRef;
import com.example.lambda.Lazy;
import com.example.logger.ExampleLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;

import static com.example.oracle.Constants.SKIP_VALUE;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/8/2021<br/>
 * Time: 8:23 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class Transactions {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    @FunctionalInterface
    public interface ForEachTransaction {
        void accept(String xid, LogMinerOperation op);
    }

    @FunctionalInterface
    public interface RowValues {
        Object get(String columnName) throws SQLException;
    }

    @FunctionalInterface
    public interface WriteRow {
        void write(LogMinerOperation op, TableRef tableRef, String rowId) throws SQLException;
    }

    public interface RetryFunction<ReturnType> extends RetryFunctionThrowable<ReturnType, RuntimeException> {
        ReturnType accept(Connection connection) throws Exception;
    }


    public interface RetryFunctionThrowable<ReturnType, ExceptionType extends Throwable> {
        ReturnType accept(Connection connection) throws Exception, ExceptionType;
    }


    public static void sleepForRetry(Function<Integer, Duration> durationFunction, int attempt, Throwable t, String callerSummary) {
        sleepForRetry(durationFunction.apply(attempt), attempt, t, callerSummary);
    }

    public static Function<Integer, Duration> sleepDurationFunction = attempt -> Duration.ofMinutes(Math.round(Math.pow(2, attempt)));
    public static Function<Integer, Duration> quickSleepDurationFunction =
            attempt -> Duration.ofSeconds(Math.round(Math.pow(3, attempt)));

//        Lazy<OracleDynamicPageSizeConfig> pageSizeConfig = new Lazy<OracleDynamicPageSizeConfig>(this::getPageSizeConfig);
//    public static Lazy<OracleDynamicPageSizeConfig> pageSizeConfig = null;

    public static Lazy<Boolean> hasDbaExtentAccess = null;
    //    Lazy<Boolean> hasDbaExtentAccess = new Lazy<>(() -> hasTableAccess("DBA_EXTENTS"));
    public static Lazy<Boolean> hasDbaTablespacesAccess = null;
    //    Lazy<Boolean> hasDbaTablespacesAccess = new Lazy<>(() -> hasTableAccess("DBA_TABLESPACES"));
    public static Lazy<Boolean> hasDbaSegmentsAccess = null;
//    Lazy<Boolean> hasDbaSegmentsAccess = new Lazy<>(() -> hasTableAccess("DBA_SEGMENTS"));

    public static void sleepForRetry(Duration waitTime, int attempt, Throwable t, String callerSummary) {

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
            throw new RuntimeException(e);
        }
    }

    public static class ChangedRowsResult {
        final long earliestUncommittedScn;
        final long lastPageSize;

        public ChangedRowsResult(long earliestUncommittedScn, long lastPageSize) {
            this.earliestUncommittedScn = earliestUncommittedScn;
            this.lastPageSize = lastPageSize;
        }
    }


    public interface LogChunkProcessor {
        long processChunk(Connection connection, long start, long end, boolean morePage) throws SQLException;
    }

    public static class ProcessRowChunkPageResult {
        public final long plannedEndScn;
        public final long actualEndScn;

        // used for dynamicScnPaging only
        public final Optional<Long> pageSize;
        public final Optional<Long> nanosPerScn;

        public ProcessRowChunkPageResult(long plannedEndScn, long actualEndScn, long pageSize, long nanosPerScn) {
            this.plannedEndScn = plannedEndScn;
            this.actualEndScn = actualEndScn;
            this.pageSize = Optional.of(pageSize);
            this.nanosPerScn = Optional.of(nanosPerScn);
        }
    }

    public static class RowPrimaryKeys {
        public final List<OracleColumn> primaryKeyColumns;
        public final List<String> oldPrimaryKeys;
        public final List<String> newPrimaryKeys;

        public RowPrimaryKeys(
                List<OracleColumn> primaryKeyColumns, List<String> oldPrimaryKeys, List<String> newPrimaryKeys) {
            this.primaryKeyColumns = primaryKeyColumns;
            this.oldPrimaryKeys = oldPrimaryKeys;
            this.newPrimaryKeys = newPrimaryKeys;
        }

        public boolean changed() {
            for (int i = 0; i < primaryKeyColumns.size(); i++) {
                if (oldPrimaryKeys.get(i).equals(SKIP_VALUE)
                        || newPrimaryKeys.get(i).equals(SKIP_VALUE)
                        || !oldPrimaryKeys.get(i).equals(newPrimaryKeys.get(i))) {
                    return true;
                }
            }
            return false;
        }

        public String getOldValue(String columnName) {
            for (int i = 0; i < primaryKeyColumns.size(); i++) {
                if (primaryKeyColumns.get(i).name.equals(columnName)) {
                    return oldPrimaryKeys.get(i);
                }
            }
            throw new RuntimeException("Column name not found. This should never happen. Check your code.");
        }

        @Override
        public String toString() {
            StringBuilder opk = new StringBuilder();
            StringBuilder npk = new StringBuilder();
            opk.append("oldPrimaryKeys {");
            npk.append("newPrimaryKeys {");
            for (int i = 0; i < primaryKeyColumns.size(); i++) {
                if (i > 0) {
                    opk.append(", ");
                    npk.append(", ");
                }
                opk.append(primaryKeyColumns.get(i).name).append('=').append(oldPrimaryKeys.get(i));
                npk.append(primaryKeyColumns.get(i).name).append('=').append(newPrimaryKeys.get(i));
            }
            opk.append('}');
            npk.append('}');
            return "RowPrimaryKeys {" + opk.toString() + ", " + npk.toString() + '}';
        }

        public static RowPrimaryKeys build(List<OracleColumn> columns, Transactions.RowValues rowValues, Map<String, Object> row)
                throws SQLException {
            List<OracleColumn> primaryKeyColumns = new ArrayList<>();
            List<String> oldPrimaryKeys = new ArrayList<>();
            List<String> newPrimaryKeys = new ArrayList<>();

            int pki = 0;
            for (OracleColumn col : columns) {
                if (col.primaryKey) {
                    Object opkObj = rowValues.get("opk" + pki++);
                    String opk = (opkObj != null) ? opkObj.toString() : null;
                    Object npkObj = row.get(col.name);
                    String npk = (npkObj != null) ? npkObj.toString() : null;
                    if (opk == null || npk == null) {
                        continue;
                    }
                    primaryKeyColumns.add(col);
                    oldPrimaryKeys.add(opk.trim());
                    newPrimaryKeys.add(npk.trim());
                }
            }

            return new RowPrimaryKeys(primaryKeyColumns, oldPrimaryKeys, newPrimaryKeys);
        }
    }

    static class ObjectFileInfo {
        final long objectId;
        final long fileNumber;

        public ObjectFileInfo(long objectId, long fileNumber) {
            this.objectId = objectId;
            this.fileNumber = fileNumber;
        }
    }
}