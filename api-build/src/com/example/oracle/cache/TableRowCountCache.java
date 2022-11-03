package com.example.oracle.cache;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.logger.event.integration.WarningEvent;
import com.example.oracle.ConnectionFactory;
import com.example.oracle.RowIdType;
import com.example.oracle.exceptions.ImportFailureException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.example.oracle.Util.doubleQuote;
import static com.example.oracle.Util.singleQuote;

/**
 * Ondemand cache, whenever the table is requested that will be cached and will be utilized in the next request
 *
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/12/2021<br/>
 * Time: 9:58 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class TableRowCountCache {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final Map<TableRef, Long> tableRowCounts = new HashMap<>();
    private static final Map<TableRef, RowIdType> tableRowIdTypeMap = new HashMap<>();

    public long getRowCount(TableRef tableRef) throws ImportFailureException {
        if (tableRowCounts.containsKey(tableRef)) {
            return tableRowCounts.get(tableRef);
        }

        Long rowCount =
                ConnectionFactory.getInstance().retry(
                        tableRef.toString(),
                        t -> new ImportFailureException("Failed while counting rows in " + tableRef, t),
                        (conn) -> {
                            Optional<Long> rowCountEstimate = getRowCountEstimate(conn, tableRef);
                            if (rowCountEstimate.isPresent()) {
                                return rowCountEstimate.get();
                            }
                            String message =
                                    "Unable to get row count quickly for '"
                                            + tableRef
                                            + "'. Falling back to counting the rows directly, which can be very slow. If counting the rows directly continues to fail, please update the statistics for '"
                                            + tableRef
                                            + "' using the DBMS_STATS package.";
                            LOG.customerWarning(WarningEvent.warning("get_row_count", message));
                            return getRowCountActual(conn, tableRef);
                        });
        tableRowCounts.put(tableRef, rowCount);
        return rowCount;
    }


    long getRowCountActual(Connection connection, TableRef tableRef) throws SQLException {
        String query = "SELECT COUNT(*) AS COUNT FROM " + doubleQuote(tableRef);

        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) throw new SQLException("No results returned from query");
            return rs.getLong("COUNT");
        }
    }

    Optional<Long> getRowCountEstimate(Connection connection, TableRef tableRef) throws SQLException {
        String query =
                "SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER="
                        + singleQuote(tableRef.schema)
                        + " AND TABLE_NAME="
                        + singleQuote(tableRef.name);

        Optional<Long> rowCount = Optional.empty();

        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                long numRows = rs.getLong("NUM_ROWS");
                if (numRows > 0) {
                    rowCount = Optional.of(numRows);
                }
            }
        }
        return rowCount;
    }


    public RowIdType getRowIdType(TableRef tableRef) throws ImportFailureException {
        if (tableRowIdTypeMap.containsKey(tableRef)) {
            return tableRowIdTypeMap.get(tableRef);
        }
        // 0 - Restricted, 1 - Extended
        int rowType =
                ConnectionFactory.getInstance().retry(
                        "Get RowIdType for " + tableRef,
                        t -> new ImportFailureException("Failed while getting the ROWID Type for " + tableRef, t),
                        (conn) -> getRowIdType(conn, tableRef));
        RowIdType rowIdType = RowIdType.valueOf(rowType);
        tableRowIdTypeMap.put(tableRef, rowIdType);
        return rowIdType;
    }


    private int getRowIdType(Connection connection, TableRef tableRef) throws SQLException {
        String query =
                "SELECT DBMS_ROWID.ROWID_TYPE(ROWID) as ROW_TYPE FROM " + doubleQuote(tableRef) + " WHERE ROWNUM = 1";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) throw new SQLException("Error while getting the ROWID Type");
            return rs.getInt("ROW_TYPE");
        }
    }
}