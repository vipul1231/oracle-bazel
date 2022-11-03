package com.example.oracle;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/8/2021<br/>
 * Time: 8:17 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public final class Constants {
    private Constants() {
    }

    // Fetch size is a setting for the JDBC driver. It tells the driver how many rows of the result set to fetch at
    // once.
    // The logical model for iterating through the result set is to get rows one at a time using the ResultSet.next()
    // method. But if the driver literally fetched one row at a time from the server, it would be very slow. On the
    // other
    // hand, if the driver fetched the entire result set at once, it would blow up the memory on the client side.
    public static final int DEFAULT_FETCH_SIZE = 50_000;
    // In Oracle RAC, as we query rows over a large block range we must wait until all row locks in that range are
    // released. We set the block range for Oracle RAC to be a small fixed value to avoid waiting too long.
    public static final int defaultRACBlockRange = 100_000;

    public static final int MAX_RETRIES = 3;
    public static final int MAX_PAGE_SIZE_SHRINK_RETRIES = 6;
    public static final int QUERY_LIST_MAX_SIZE = 1000;

    public static final int MIN_BLOCK_ROW_NUMBER = 0;
    // 32k -1 is the max valid row number for a 32k block size. 32k is the max limit for Block size
    public static final int MAX_BLOCK_ROW_NUMBER = 32767;

    public static final long PAGE_SIZE_CONFIG_HOURS = 24;

    public static final long INVALID_SCN = -1L;

    public static final int VERSION_QUERY_PARALLEL_HINT_DEGREE = 8;
    public static final long VERSION_QUERY_PARALLEL_HINT_MIN_ROW_COUNT = 5_000_000L;

    public static final String ROW_ID_COLUMN_NAME = "example_ROW_ID__";

    public static final int MAX_INVALID_NUMBER_WARNING_BEFORE_GIVING_UP = 100;
    public static int invalidNumberWarningCount = 0;

    public static final String SYSTEM_SCHEMAS =
            "'SYS','SYSTEM','UNKNOWN','OUTLN','SCOTT','ADAMS','JONES',"
                    + "'CLARK','BLAKE','DEMO','AWR_STAGE',"
                    + "'CSMIG','CTXSYS','DBSNMP','DIP','DMSYS','DSSYS',"
                    + "'EXFSYS','LBACSYS','MDSYS','ORACLE_OCM','ORDPLUGINS',"
                    + "'ORDSYS','PERFSTAT','TRACESVR','TSMSYS','XDB','RDSADMIN',"
                    + "'ANONYMOUS','APEX_030200','APEX_PUBLIC_USER','APPQOSSYS',"
                    + "'DVSYS','FLOWS_FILES','MDDATA','MGMT_VIEW','ORDDATA',"
                    + "'OWBSYS','OWBSYS_AUDIT','SI_INFORMTN_SCHEMA',"
                    + "'SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','SYSMAN',"
                    + "'WKSYS','WMSYS','APEX_040200','AUDSYS','DVF','DVSYS','GSMADMIN_INTERNAL',"
                    + "'GSMCATUSER','GSMUSER','SYSBACKUP','SYSDG','SYSKM', 'XS$NULL',"
                    + "'OJVMSYS', 'OLAPSYS', 'FRANCK', 'RMAN', 'SQLTXADMIN', 'SQLTXPLAIN'";

    /**
     * Usually Oracle database sample schemas: http://docs.oracle.com/database/121/COMSC/installation.htm
     * But a customer can use them for anything if they so wish
     */
    public static List<String> sampleSchemas = Arrays.asList("HR", "OE", "PM", "SH", "IX", "BI");

    public enum KeyType {
        PRIMARY,
        FOREIGN
    }

    public enum LogDirection {
        REDO_VALUE,
        UNDO_VALUE,
    }

    public static final Duration INCOMPLETE_INTERVAL = Duration.ofMinutes(15);
    public static final Duration UPPER_BOUND_INTERVAL = Duration.ofHours(6);
    public static Duration upperBoundIntervalForUncommittedTransaction = UPPER_BOUND_INTERVAL;


    public static void setUpperBoundIntervalForUncommittedTransaction(Duration duration) {
        upperBoundIntervalForUncommittedTransaction = duration;
    }

    /*
     * ROWIDs are 18-characters long. Based on observation, rowids ending with
     * 12 or more 'A's are invalid. Rowids composed of all 'A's are also invalid.
     */
    public static final Pattern INVALID_ROWID = Pattern.compile(".*(?:[A]{12,})$");

    public static final String SKIP_VALUE = "skip-9Qq2Fa";
}