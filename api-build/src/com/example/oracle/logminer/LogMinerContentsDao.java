package com.example.oracle.logminer;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.oracle.Constants;
import com.example.oracle.LogMinerOperation;
import com.example.oracle.Transactions;

import java.sql.*;
import java.util.*;

import static com.example.oracle.Constants.DEFAULT_FETCH_SIZE;
import static com.example.oracle.Constants.SYSTEM_SCHEMAS;
import static com.example.oracle.Util.binaryXidToString;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:38 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class LogMinerContentsDao {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final String SELECT_PERMISSIONS_CHECK = "SELECT * FROM SYS.V_$LOGMNR_CONTENTS WHERE 1=0";

    private static final String CHECK_FOR_NON_EMPTY_CONTENTS =
            "SELECT scn FROM SYS.V_$LOGMNR_CONTENTS where ROWNUM = 1";

    /** ROWNUM is required to stop mining the logs as soon as we find the transaction. */
    private static final String SELECT_LAST_TRANSACTION =
            "SELECT OPERATION_CODE, XID FROM SYS.V_$LOGMNR_CONTENTS "
                    + "WHERE OPERATION_CODE in (7,36) AND SEG_OWNER IS NULL "
                    + "AND XID = hextoraw(':xid') AND ROWNUM = 1";

    private Connection connection;

    public LogMinerContentsDao(Connection conn) {
        this.connection = conn;
    }

    /**
     * Look for COMMIT and ROLLBACK entries in the logs
     */
    public Map<String, LogMinerOperation> completedTransactions(Connection connection) throws SQLException {
        LOG.info("Getting list of all completed transactions");

//        @Language("SQL")
        String query =
                "SELECT OPERATION_CODE, XID FROM SYS.V_$LOGMNR_CONTENTS "
                        + "WHERE OPERATION_CODE in (7,36) AND SEG_OWNER IS NULL";

        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(query)) {
            rows.setFetchSize(Constants.DEFAULT_FETCH_SIZE);

            Map<String, LogMinerOperation> completed = new HashMap<>();

            readCompletedTransactions(rows, completed::put);

            return completed;
        }
    }

    public Set<TableRef> getModifiedTables(Connection connection, Set<TableRef> selected) throws SQLException {
        LOG.info("Getting list of modified tables");

        Set<TableRef> modifiedTables = new HashSet<>();

//        @Language("SQL")
        String query =
                "SELECT SEG_OWNER, TABLE_NAME FROM SYS.V_$LOGMNR_CONTENTS "
                        + "WHERE OPERATION_CODE in (1,2,3) AND "
                        + "SEG_OWNER NOT IN ("
                        + SYSTEM_SCHEMAS
                        + ")";

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query)) {
            results.setFetchSize(DEFAULT_FETCH_SIZE);

            while (results.next()) {
                String schema = results.getString("SEG_OWNER");
                String table = results.getString("TABLE_NAME");
                TableRef tableRef = new TableRef(schema, table);

                if (selected.contains(tableRef)) modifiedTables.add(tableRef);
            }

            return modifiedTables;
        }
    }

    public void readCompletedTransactions(ResultSet rows, Transactions.ForEachTransaction forEach) throws SQLException {
        while (rows.next()) {
            String xid = binaryXidToString((byte[]) rows.getObject("XID"));
            LogMinerOperation op = LogMinerOperation.valueOf(rows.getBigDecimal("OPERATION_CODE").intValue());
            forEach.accept(xid, op);
        }
    }

    /**
     * If permissions are set correctly then this method returns normally, otherwise a SQLException is thrown.
     *
     * @throws SQLException
     */
    public void checkSelectPermission() throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_PERMISSIONS_CHECK)) {
            ps.executeQuery();
        }
    }

    /**
     * @return true if the query does not return any results - indicating that the view has no content
     * @throws SQLException
     */
    public boolean isContentEmpty() throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(CHECK_FOR_NON_EMPTY_CONTENTS);
             ResultSet rs = ps.executeQuery(); ) {
            return !rs.next();
        }
    }

    static String selectLastTransactionQuery(String xId) {
        return SELECT_LAST_TRANSACTION.replace(":xid", xId);
    }

    /**
     * Refactored from OracleApi
     *
     * @param xId
     * @return
     * @throws SQLException
     */
    public Optional<LogMinerOperation> getTransactionStatus(String xId) throws SQLException {
        String query = selectLastTransactionQuery(xId);

        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet rows = statement.executeQuery()) {

            if (rows.next()) {
                return Optional.of(LogMinerOperation.valueOf(rows.getInt("OPERATION_CODE")));
            }

            return Optional.empty();
        }
    }
}