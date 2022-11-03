package com.example.oracle.logminer;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:30 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

import com.example.flag.FeatureFlag;
import com.example.logger.ExampleLogger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/** Refactored from OracleAPI */
public class ArchivedLogDao {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final String CHECK_SELECT_PERMISSIONS = "SELECT * FROM SYS.V_$ARCHIVED_LOG WHERE 1=0";

    private static final String LAST_SCN_QUERY =
            "SELECT NEXT_CHANGE#-1 AS LAST_SCN FROM SYS.V_$ARCHIVED_LOG "
                    + "WHERE STANDBY_DEST = 'NO' "
                    + "ORDER BY FIRST_CHANGE# DESC";

    private static final String LAST_SCN_CREATOR_ARCH_QUERY =
            "SELECT NEXT_CHANGE#-1 AS LAST_SCN FROM SYS.V_$ARCHIVED_LOG "
                    + "WHERE STANDBY_DEST = 'NO' AND CREATOR = 'ARCH' "
                    + "ORDER BY FIRST_CHANGE# DESC";

    private static final String ARCHIVED_LOG_TIME_QUERY_EXPR =
            "SELECT NAME,FIRST_TIME,NEXT_TIME,FIRST_CHANGE#,NEXT_CHANGE#,SEQUENCE# FROM SYS.V_$ARCHIVED_LOG"
                    + " WHERE STANDBY_DEST = 'NO' AND CREATOR = 'ARCH'"
                    + " AND ((FIRST_TIME <= :timestamp AND NEXT_TIME > :timestamp) OR FIRST_TIME > :timestamp)"
                    + " ORDER BY FIRST_CHANGE# ASC";

    private static final String ARCHIVED_LOG_SCN_QUERY_EXPR =
            "SELECT NAME,FIRST_TIME,NEXT_TIME,FIRST_CHANGE#,NEXT_CHANGE#,SEQUENCE# FROM SYS.V_$ARCHIVED_LOG"
                    + " WHERE STANDBY_DEST = 'NO' AND CREATOR = 'ARCH'"
                    + " AND ((FIRST_CHANGE# <= :scn AND NEXT_CHANGE# > :scn) OR FIRST_CHANGE# > :scn)"
                    + " ORDER BY FIRST_CHANGE# ASC";

    private Connection connection;

    public ArchivedLogDao(Connection connection) {
        this.connection = connection;
    }

    /**
     * If permissions are set correctly then this method returns normally, otherwise a SQLException is thrown.
     *
     * @throws SQLException
     */
    public void checkSelectPermissions() throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(CHECK_SELECT_PERMISSIONS)) {
            ps.executeQuery();
        }
    }

    public long getEndScn() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(getLastScnQuery())) {

            if (!result.next()) throw new RuntimeException("getEndScn() query reply is empty");

            return result.getBigDecimal("LAST_SCN").longValue();
        }
    }

    String getLastScnQuery() {
        if (FeatureFlag.check("OracleMostRecentScnQueryFilter")) {
            return LAST_SCN_CREATOR_ARCH_QUERY;
        }

        return LAST_SCN_QUERY;
    }

    public List<ArchivedLog> queryByTimestamp(String timestamp) throws SQLException {
        return query(ARCHIVED_LOG_TIME_QUERY_EXPR.replace(":timestamp", timestamp));
    }

    public List<ArchivedLog> queryByScnRange(long minScn) throws SQLException {
        return query(ARCHIVED_LOG_SCN_QUERY_EXPR.replace(":scn", Long.toString(minScn)));
    }

    private List<ArchivedLog> query(String statement) throws SQLException {
        List<ArchivedLog> set = new ArrayList<>();

        LOG.info("[DEBUG] archived log query: " + statement);
        try (PreparedStatement ps = connection.prepareStatement(statement)) {
            try (ResultSet resultSet = ps.executeQuery()) {

                while (resultSet.next()) {
                    set.add(ArchivedLog.from(resultSet));
                }
            }
        }

        return set;
    }
}
