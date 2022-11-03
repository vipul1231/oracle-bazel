package com.example.oracle.logminer;

import com.example.oracle.ConnectionFactory;
import com.example.oracle.OracleSetupForm;
import com.example.oracle.Retry;
import com.example.oracle.SetupValidator;

import java.sql.Connection;
import java.sql.SQLException;

import static com.example.oracle.logminer.LogMinerSession.sysdateMinus;
import static com.example.oracle.logminer.LogMinerSession.sysdateWithOffset;

/**
 * Helper class for Logminer setup validations
 *
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/9/2021<br/>
 * Time: 7:05 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class LogMinerSetupValidator extends SetupValidator {

    public static void validate(LogMinerSession logMinerSession) {
        checkArchivedLogs(logMinerSession);
        checkLogmnrContents(logMinerSession);
        checkStartLogminer(logMinerSession);
        checkLogminerLogs(logMinerSession);
        // If an exception prevents us from reaching this last step then LogMinerSession
        // will attempt to end the logminer session automatically.
        checkStopLogminer(logMinerSession);
    }

    static void checkLogminerLogs(LogMinerSession logMinerSession) {
        try {
            if (logMinerSession.getLogMinerContentsDao().isContentEmpty()) {
                throw new RuntimeException("V_$LOGMNR_CONTENTS query result is empty");
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("insufficient privileges"))
                throw new RuntimeException("Missing SELECT ANY TRANSACTION permission");
            else throw new RuntimeException("Could not read from SYS.V_$LOGMNR_CONTENTS: " + e.getMessage());
        }
    }

    public static void checkLogmnrContents(LogMinerSession logMinerSession) {
        try {
            logMinerSession.getLogMinerContentsDao().checkSelectPermission();
        } catch (SQLException e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for SYS.V_$LOGMNR_CONTENTS");
            else throw new RuntimeException("Could not read from SYS.V_$LOGMNR_CONTENTS: " + e.getMessage());
        }
    }

    public static void checkArchivedLogs(LogMinerSession logMinerSession) {
        try {
            logMinerSession.getArchivedLogDao().checkSelectPermissions();
        } catch (SQLException e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for SYS.V_$ARCHIVED_LOG");
            else throw new RuntimeException("Could not read from SYS.V_$ARCHIVED_LOG: " + e.getMessage());
        }
    }

    public static void checkStartLogminer(LogMinerSession logMinerSession) {
        try {
            logMinerSession.timeSpan(sysdateMinus("0.125"), sysdateMinus("0.1")).start();
        } catch (SQLException e) {
            if (e.getMessage().contains("ORA-01291: missing logfile")
                    || e.getMessage()
                    .contains("ORA-01292: no log file has been specified for the current LogMiner session"))
                throw new RuntimeException(
                        "Archive log retention period is less than 3 hours or logs have not accumulated for 3 hours");
            else if (e.getMessage().contains("identifier 'DBMS_LOGMNR' must be declared"))
                throw new RuntimeException("Missing EXECUTE permission on DBMS_LOGMNR");
            else throw new RuntimeException("Could not execute DBMS_LOGMNR.START_LOGMNR: " + e.getMessage());
        }
    }

    public static void checkStopLogminer(LogMinerSession logMinerSession) {
        try {
            logMinerSession.stop();
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute DBMS_LOGMNR.END_LOGMNR: " + e.getMessage());
        }
    }

    public int getMaxLogminerDuration() {
        return Retry.act(
                (ans) -> {
                    int maxDuration = 0;
                    try (Connection connection = ConnectionFactory.getInstance().connectToSourceDb();
                         LogMinerSession logMinerSession = new LogMinerSession(connection); ) {
                        for (int i = 3;
                             i < OracleSetupForm.ARCHIVE_LOG_RETENTION_HOURS_MAX_CHECK;
                             i += i < 12 ? 1 : 6) {
                            if (checkStartLogminerVariable(logMinerSession, i)) {
                                maxDuration = i;
                            } else {
                                break;
                            }
                        }
                        return maxDuration;
                    }
                });
    }

    private boolean checkStartLogminerVariable(LogMinerSession logMinerSession, int hoursAgo) {
        try {
            logMinerSession.timeSpan(sysdateWithOffset(hoursAgo), sysdateWithOffset(hoursAgo - 1)).start();

        } catch (SQLException e) {
            if (e.getMessage().contains("ORA-01291: missing logfile")
                    || e.getMessage()
                    .contains("ORA-01292: no log file has been specified for the current LogMiner session"))
                return false;
            else if (e.getMessage().contains("identifier 'DBMS_LOGMNR' must be declared"))
                throw new RuntimeException("Missing EXECUTE permission on DBMS_LOGMNR");
            else throw new RuntimeException("Could not execute DBMS_LOGMNR.START_LOGMNR: " + e.getMessage());
        }

        return true;
    }
}