package com.example.oracle;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.oracle.cache.PartitionedTableCache;
import com.example.oracle.exceptions.ImportFailureException;
import com.example.oracle.exceptions.QuickBlockRangeFailedException;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;

import static com.example.oracle.Constants.MAX_BLOCK_ROW_NUMBER;
import static com.example.oracle.Constants.MIN_BLOCK_ROW_NUMBER;
import static com.example.oracle.OracleErrorCode.ORA_06564;
import static com.example.oracle.SqlUtil.getTablespaceTypes;
import static com.example.oracle.Transactions.hasDbaExtentAccess;
import static com.example.oracle.Transactions.hasDbaTablespacesAccess;
import static com.example.oracle.Util.doubleQuote;

/**
 * DBA_EXTENTS keeps track of where a table (or table segment) exists on the disk.
 * <p>
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/12/2021<br/>
 * Time: 10:50 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class DbaExtendsHandler {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    /**
     * Executes the slow block range query (can be used as a fallback approach if quicklyGetTableBlockRange does not
     * return any results).
     *
     * <p>This approach does a Full Table Scan to get the range slowly. This requires no special access but is very slow
     * for big tables.
     *
     * @param table
     * @return
     */
    Optional<BlockRange> slowlyGetTableBlockRange(TableRef table) {
        /*
         * The actual tablespace type doesn't really matter. What matters is that the type is consistent. If we find
         * the range using SMALLFILE, we need to make sure we construct our paging queries also using SMALLFILE.
         */
        TablespaceType tablespaceType = TablespaceType.SMALLFILE;

        /*
         * DBMS_ROWID.ROWID_BLOCK_NUMBER is an interesting function. It doesn't actually look up data. It takes the
         * ROWID, deserializes it and interprets the block_number from the deserialized value. Interestingly, specifying
         * BIGFILE or SMALLFILE just changes the way the block_number in interpreted. Somewhat like changing a binary
         * number into base 10 vs base 6. This is why it doesn't matter what the actual storage engine uses. We are just
         * specifying what formula to use when calculating the block_number.
         */

//        @Language("SQL")
        String query =
                "SELECT  MIN(DBMS_ROWID.ROWID_BLOCK_NUMBER(ROWID, ?)) AS MIN_BLOCK_NUMBER, "
                        + "        MAX(DBMS_ROWID.ROWID_BLOCK_NUMBER(ROWID, ?)) AS MAX_BLOCK_NUMBER FROM "
                        + doubleQuote(table);

        Transactions.RetryFunction<Optional<BlockRange>> action =
                (connection) -> {
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        statement.setString(1, tablespaceType.toString());
                        statement.setString(2, tablespaceType.toString());
                        try (ResultSet result = statement.executeQuery()) {

                            if (!result.next()) throw new RuntimeException("getTableBlockRange query result is empty");

                            BigDecimal minBlockNumber = result.getBigDecimal("MIN_BLOCK_NUMBER");
                            BigDecimal maxBlockNumber = result.getBigDecimal("MAX_BLOCK_NUMBER");

                            if (minBlockNumber == null)
                                if (maxBlockNumber == null) return Optional.empty();
                                else
                                    throw new IllegalStateException(
                                            "If min_block_number is null, max_block_number should be null too");

                            return Optional.of(
                                    new BlockRange(
                                            minBlockNumber.longValue(), maxBlockNumber.longValue(), tablespaceType));
                        }
                    } catch (Exception e) {
                        if (ORA_06564.is(e.getMessage())) { // object doest not exist
                            LOG.log(Level.WARNING, "Table cannot be imported: " + table, e);
                            return Optional.empty();
                        }
                        throw e;
                    }
                };

        return ConnectionFactory.getInstance().retry(
                "get table block range on " + table.toString(),
                t -> new RuntimeException("Error in getting table block range: " + table, t),
                action);
    }


    List<Transactions.ObjectFileInfo> getObjectFileInfoFromBlocks(TableRef tableRef, BlockRange pageBlockRange) {
        return ConnectionFactory.getInstance().retry(
                "Get objectId and fileNumber with in the current block range for " + tableRef,
                t ->
                        new RuntimeException(
                                "Failed to get objectId and fileNumber from block "
                                        + pageBlockRange.min
                                        + " to "
                                        + pageBlockRange.max
                                        + " for table "
                                        + tableRef,
                                t),
                (conn) -> getObjectFileDetails(conn, tableRef, pageBlockRange));
    }

    private PreparedStatement selectObjectAndFileNumbersWithInTheBlockRange(
            Connection connection, TableRef tableRef, BlockRange pageBlockRange) throws SQLException {

        // Only required for partitioned tables
        String partitionCondition =
                PartitionedTableCache.getPartitionedTables().contains(tableRef) ? " AND obj.SUBOBJECT_NAME = ext.PARTITION_NAME" : "";

//        @Language("SQL")
        String query =
                "SELECT obj.DATA_OBJECT_ID as OBJECT_ID,"
                        + " ext.RELATIVE_FNO as FILE_NUMBER"
                        + " FROM ALL_OBJECTS obj"
                        + " JOIN DBA_EXTENTS ext"
                        + " ON obj.OWNER = ext.OWNER"
                        + " AND obj.OBJECT_NAME = ext.SEGMENT_NAME"
                        + partitionCondition
                        + " AND ext.SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')"
                        + " WHERE obj.OWNER = ?"
                        + " AND obj.OBJECT_NAME = ?"
                        + " AND ((ext.BLOCK_ID BETWEEN ? AND ?) OR (ext.BLOCK_ID + ext.BLOCKS BETWEEN ? AND ?))"
                        + " AND obj.DATA_OBJECT_ID IS NOT NULL"
                        + " GROUP BY obj.DATA_OBJECT_ID, ext.RELATIVE_FNO"
                        + " ORDER BY obj.DATA_OBJECT_ID, ext.RELATIVE_FNO";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, tableRef.schema);
        statement.setString(2, tableRef.name);
        statement.setBigDecimal(3, new BigDecimal(pageBlockRange.min));
        statement.setBigDecimal(4, new BigDecimal(pageBlockRange.max));
        statement.setBigDecimal(5, new BigDecimal(pageBlockRange.min));
        statement.setBigDecimal(6, new BigDecimal(pageBlockRange.max));
        return statement;
    }


    private List<Transactions.ObjectFileInfo> getObjectFileDetails(Connection conn, TableRef tableRef, BlockRange pageBlockRange)
            throws SQLException {

        List<Transactions.ObjectFileInfo> objFileNoList = new ArrayList<>();
        try (PreparedStatement statement =
                     selectObjectAndFileNumbersWithInTheBlockRange(conn, tableRef, pageBlockRange);
             ResultSet rows = statement.executeQuery()) {

            while (rows.next()) {
                long objectId = rows.getLong("OBJECT_ID");
                long fileNumber = rows.getLong("FILE_NUMBER");
                if (fileNumber != 0) {
                    Transactions.ObjectFileInfo objFileNo = new Transactions.ObjectFileInfo(objectId, fileNumber);
                    objFileNoList.add(objFileNo);
                }
            }
        }
        return objFileNoList;
    }


    /**
     * Return the block range for the given table. This method first attempts to use the fast approach
     * (quicklyGetTableBlockRange) and falls back to the slow approach (slowlyGetTableBlockRange) if the fast approach
     * doesn't provide any results.
     *
     * @param table
     * @return optional BlockRange
     */
    Optional<BlockRange> getTableBlockRange(TableRef table) {
        // Try the fast strategy first
        Optional<BlockRange> blockRange = quicklyGetTableBlockRange(table);
        if (blockRange.isPresent()) {
            return blockRange;
        }

        // Fallback to slow approach
        LOG.warning("Getting block range quickly failed, falling back to querying table directly");

        return slowlyGetTableBlockRange(table);
    }

    /**
     * This fast approach attempts to use system views to get the range quickly. It uses a combination of VIEWs in an
     * attempt to figure out the block_id range and the tablespace type.
     *
     * <p>This requires that the customer has set up access to those views. If access has not been granted or if the
     * queries fail to determine the range and tablespace type then an empty response is returned.
     *
     * @param table table for which we want to get the block_id range and the tablespace type.
     * @return The block range, if views are accessible and block_id range can be determined, otherwise empty.
     */
    Optional<BlockRange> quicklyGetTableBlockRange(TableRef table) {
        try {
            return quicklyGetTableBlockRangeInner(table);
        } catch (QuickBlockRangeFailedException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            return Optional.empty();
        }
    }

    Optional<BlockRange> quicklyGetTableBlockRangeInner(TableRef table) throws QuickBlockRangeFailedException {
        BlockRange range = new BlockRange();

        if (!hasDbaExtentAccess.get() || !hasDbaTablespacesAccess.get()) {
            // MUST have DBA_EXTENTS and DBA_TABLESPACES access
            return Optional.empty();
        }

        /*
         * We need to get all the possible tablespace types for a table. Tables can be partitioned and spread
         * across multiple tablespaces. We only really want to know if they are all the same type and what that type
         * is.
         */
        Set<TablespaceType> tablespaceTypes = getTablespaceTypes(table);

        if (tablespaceTypes.isEmpty()) {
            // no tablespace types, shouldn't be possible
            return Optional.empty();
        }

        if (tablespaceTypes.size() > 1) {
            // table is on two different tablespace types. we cannot support this currently
            return Optional.empty();
        }

        range.type = Iterables.getOnlyElement(tablespaceTypes);

        /*
         * DBA_EXTENTS keeps track of where a table (or table segment) exists on the disk. It keeps track of the
         * starting block_number and how many blocks into the extent the table has written into. Most tables have
         * multiple entries in this view, but we only want to absolute min and max values.
         */

//        @Language("SQL")
        String blockQuery =
                "SELECT"
                        + " MIN(BLOCK_ID) AS MIN_BLOCK_ID, "
                        + " MAX(BLOCK_ID + BLOCKS) AS MAX_BLOCK_ID "
                        + " FROM DBA_EXTENTS "
                        + " WHERE OWNER = ? "
                        + " AND SEGMENT_NAME = ? "
                        + " AND SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')";

        Transactions.RetryFunction<Optional<BlockRange>> action =
                (connection) -> {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(blockQuery)) {
                        preparedStatement.setString(1, table.schema);
                        preparedStatement.setString(2, table.name);
                        try (ResultSet result = preparedStatement.executeQuery()) {
                            if (!result.next()) return Optional.empty();

                            range.min = result.getLong("MIN_BLOCK_ID");
                            range.max = result.getLong("MAX_BLOCK_ID");
                            return Optional.of(range);
                        }
                    }
                };

        return ConnectionFactory.getInstance().retry(
                "quickly get table block range on " + table.toString(),
                t -> new RuntimeException("Error in getting table block range: " + table, t),
                action);
    }


    String getMinRowIdFromBlockInfo(
            TableRef tableRef, RowIdType rowIdType, Transactions.ObjectFileInfo objectFileInfo, BlockRange pageBlockRange)
            throws ImportFailureException {
        return getRowIdFromBlockInfo(tableRef, rowIdType, objectFileInfo, pageBlockRange.min, MIN_BLOCK_ROW_NUMBER);
    }

    String getMaxRowIdFromBlockInfo(
            TableRef tableRef, RowIdType rowIdType, Transactions.ObjectFileInfo objectFileInfo, BlockRange pageBlockRange)
            throws ImportFailureException {
        return getRowIdFromBlockInfo(tableRef, rowIdType, objectFileInfo, pageBlockRange.max, MAX_BLOCK_ROW_NUMBER);
    }

    private String getRowIdFromBlockInfo(
            TableRef tableRef, RowIdType rowIdType, Transactions.ObjectFileInfo objectFileInfo, long blockNumber, long rowNumber)
            throws ImportFailureException {
        return ConnectionFactory.getInstance().retry(
                "Get RowId for " + tableRef,
                t -> new ImportFailureException("Failed while getting the rowId for " + tableRef, t),
                (conn) -> getRowId(conn, tableRef, rowIdType, objectFileInfo, blockNumber, rowNumber));
    }

    String getRowId(
            Connection connection,
            TableRef tableRef,
            RowIdType rowIdType,
            Transactions.ObjectFileInfo objectFileInfo,
            long blockNumber,
            long rowNumber)
            throws SQLException {

//        @Language("SQL")
        String query = "SELECT DBMS_ROWID.ROWID_CREATE(?,?,?,?,?) AS ROW_ID FROM DUAL";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, rowIdType.code);
            statement.setBigDecimal(2, new BigDecimal(objectFileInfo.objectId));
            statement.setBigDecimal(3, new BigDecimal(objectFileInfo.fileNumber));
            statement.setBigDecimal(4, new BigDecimal(blockNumber));
            statement.setBigDecimal(5, new BigDecimal(rowNumber));

            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next())
                    throw new SQLException(
                            "Error while creating the ROWID from object, file no, block for table " + tableRef);
                return rs.getString("ROW_ID");
            }
        }
    }

}