package com.example.oracle.logminer;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:26 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Encapsulates starting and ending LogMiner. Refactored from OracleApi */
public class LogMinerSession implements AutoCloseable {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final String ADD_NEWLOGFILE =
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?,OPTIONS => DBMS_LOGMNR.NEW);END;";

    private static final String ADD_LOGFILE =
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?,OPTIONS => DBMS_LOGMNR.ADDFILE);END;";

    public boolean isContinuousMineEnabled() {
        return this.continuousMineEnabled;
    }

    /**
     * @TODO need to write business logic for same
     * @return
     */
    public int getRedoLogFileSize() {
        return  0;
    }

    public enum Option {
        /** CONTINUOUS_MINE = automatically load log files that match the SCN range */
        CONTINUOUS_MINE("DBMS_LOGMNR.CONTINUOUS_MINE"),
        /**
         * DICT_FROM_ONLINE_CATALOG = translate obj#id and columns numbers to *current* names (DDL events break this)
         */
        DICT_FROM_ONLINE_CATALOG("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"),
        /** NO_ROWID_IN_STMT = removed ROWID form SQL_REDO/UNDO. (candidate for removal) */
        NO_ROWID_IN_STMT("DBMS_LOGMNR.NO_ROWID_IN_STMT"),
        /** SKIP_CORRUPTION = skip corrupted files */
        SKIP_CORRUPTION("DBMS_LOGMNR.SKIP_CORRUPTION"),
        /**
         * STRING_LITERALS_IN_STMT = (12c+) numbers / datetime and interval columns are literals (candidate for removal)
         */
        STRING_LITERALS_IN_STMT("DBMS_LOGMNR.STRING_LITERALS_IN_STMT");

        private String expression;

        Option(String expression) {
            this.expression = expression;
        }

        public String getExpression() {
            return expression;
        }
    }

    private boolean logMinerStarted;

    private Connection connection;

    private List<Option> startOptions = new ArrayList<>();

    private boolean continuousMineEnabled;

    public interface SessionSpan {
        /**
         * Return the span as SQL to be included in the LogMiner start procedure call
         *
         * @return
         */
        String startParams();

        /**
         * Invoke appropriate query method on dao to add log files
         *
         * @return
         * @throws SQLException
         */
        List<ArchivedLog> query() throws SQLException;
    }

    /** Used to query the archived logs by SCN range as well as start logminer with a SCN range */
    class ScnSpan implements SessionSpan {
        private Long startScn;
        private Long endScn;

        /**
         * @param startScn
         * @param endScn null is OK
         */
        ScnSpan(Long startScn, Long endScn) {
            this.startScn = startScn;
            this.endScn = endScn;
        }

        @Override
        public String startParams() {
            return interpolate(null != endScn ? "STARTSCN => :startScn, ENDSCN => :endScn," : "STARTSCN => :startScn,");
        }

        private String interpolate(String queryTemplate) {
            return queryTemplate.replace(":startScn", startScn.toString()).replace(":endScn", endScn.toString());
        }

        @Override
        public List<ArchivedLog> query() throws SQLException {
            return getArchivedLogDao().queryByScnRange(startScn);
        }
    }

    /** Used to query the archived logs by timestamp range as well as start logminer with a timestamp range */
    class TimeSpan implements SessionSpan {
        private String startTime;
        private String endTime;

        TimeSpan(String startTime, String endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public String startParams() {
            return "STARTTIME => :startTime, ENDTIME => :endTime,"
                    .replace(":startTime", startTime)
                    .replace(":endTime", endTime);
        }

        @Override
        public List<ArchivedLog> query() throws SQLException {
            return getArchivedLogDao().queryByTimestamp(startTime);
        }
    }

    private SessionSpan sessionSpan;
    private final ArchivedLogDao archivedLogDao;
    private final LogMinerContentsDao logMinerContentsDao;

    public LogMinerSession(Connection connection) {
        this.continuousMineEnabled = !FlagName.OracleLogMinerNoContinuousMine.check();
        if (continuousMineEnabled) {
            startOptions.add(Option.CONTINUOUS_MINE);
        }
        startOptions.add(Option.DICT_FROM_ONLINE_CATALOG);
        this.connection = connection;
        this.archivedLogDao = new ArchivedLogDao(connection);
        this.logMinerContentsDao = new LogMinerContentsDao(connection);
    }

    public LogMinerSession scnSpan(long start, long end) {
        this.sessionSpan = new ScnSpan(start, end);
        return this;
    }

    public LogMinerSession options(Option... options) {
        startOptions.addAll(Arrays.asList(options));
        return this;
    }

    public LogMinerSession advancedOptions() {
        return options(
                LogMinerSession.Option.NO_ROWID_IN_STMT,
                LogMinerSession.Option.SKIP_CORRUPTION,
                LogMinerSession.Option.STRING_LITERALS_IN_STMT);
    }

    public LogMinerSession timeSpan(String start, String end) {
        this.sessionSpan = new TimeSpan(start, end);

        return this;
    }

    private void addLogFiles() throws SQLException {
        List<ArchivedLog> archivedLogSet =
                Objects.requireNonNull(sessionSpan, "call scnSpan() or timeSpan() first").query();

        boolean firstArchive = true;

        for (ArchivedLog archiveLog : archivedLogSet) {
            addLogFile(firstArchive, archiveLog);
            firstArchive = false;
        }
    }

    private void addLogFile(boolean newLogfile, ArchivedLog archivedLog) throws SQLException {
        String statement = newLogfile ? ADD_NEWLOGFILE : ADD_LOGFILE;

        LOG.info("[DEBUG] addLogFile " + archivedLog);
        try (CallableStatement callableStatement = connection.prepareCall(statement)) {
            callableStatement.setString(1, archivedLog.getName());
            callableStatement.execute();
        }
    }

    /**
     * Start or restart LogMiner
     *
     * <p>11g https://docs.oracle.com/cd/B28359_01/server.111/b28319/logminer.htm#i1014404 12c
     * https://docs.oracle.com/database/121/ARPLS/d_logmnr.htm#ARPLS66784
     */
    public void start() throws SQLException {

        if (!continuousMineEnabled) {
            addLogFiles();
        }

        String startParams = null != sessionSpan ? sessionSpan.startParams() : "";
        String options = startOptions.stream().map(Option::getExpression).collect(Collectors.joining(" + "));
        String startLogminerStatement =
                "BEGIN DBMS_LOGMNR.START_LOGMNR(" + startParams + "OPTIONS => " + options + ");END;";
        try (CallableStatement cstmt = connection.prepareCall(startLogminerStatement)) {
            cstmt.execute();

            logMinerStarted = true;
        }
    }

    public void stop() throws SQLException {
        if (!logMinerStarted) return;

        try (CallableStatement cstmt = connection.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")) {
            cstmt.execute();
        } finally {
            logMinerStarted = false;
        }
    }

    public void stopQuietly() {
        try {
            stop();
        } catch (SQLException e) {
            LOG.info("Failed to stop log miner (non-fatal): " + e.getMessage());
        }
    }

    @Override
    public void close() {
        stopQuietly();
    }

    public boolean isLogMinerStarted() {
        return logMinerStarted;
    }

    public LogMinerContentsDao getLogMinerContentsDao() {
        return logMinerContentsDao;
    }

    public ArchivedLogDao getArchivedLogDao() {
        return archivedLogDao;
    }

    public SessionSpan getSessionSpan() {
        return sessionSpan;
    }

    public static BigDecimal normalizeTime(double hours) {
        return BigDecimal.valueOf(hours).divide(new BigDecimal(24), 3, BigDecimal.ROUND_HALF_UP);
    }

    public static String sysdateMinus(String value) {
        return "SYSDATE-" + value;
    }

    public static String sysdateWithOffset(double hours) {
        return sysdateMinus(normalizeTime(hours).toString());
    }
}