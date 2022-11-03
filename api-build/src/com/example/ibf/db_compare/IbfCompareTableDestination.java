package com.example.ibf.db_compare;

import com.example.core.annotations.DataType;
import com.example.ibf.IbfTableInspector;
import com.example.snowflakecritic.ibf.IbfCompareIntermediateRow;
import com.example.snowflakecritic.ibf.InvertibleBloomFilter;

import java.sql.SQLException;
import java.util.List;

public interface IbfCompareTableDestination extends IbfTableInspector {
    /**
     * get key lengths *
     */
    List<Integer> keyLengths();

    /**
     * get data types of primary keys *
     */
    List<DataType> keyTypes();

    /**
     * Fetch an InvertibleBloomFilter representing a table from a destination database by running a SQL query. The
     * InvertibleBloomFilter fetched by this method must use the common representation so it can be used to compare data
     * between different types of databases.
     *
     * @param cellCount
     * @return InvertibleBloomFilter
     * @throws SQLException
     */
    InvertibleBloomFilter fetchCommonIbf(int cellCount, List<IbfCompareColumn> sourceColumns) throws SQLException;

    /**
     * Fetch the intermediate rows from the table. The values in these rows are the input values used for generating the
     * IBF.
     *
     * <p>This method is intended for development/debugging purposes only. It will not be performant for production use
     * cases.
     *
     * @param sourceColumns
     * @return List<IbfCompareIntermediateRow>
     * @throws SQLException
     */
    default List<IbfCompareIntermediateRow> fetchIntermediateRowsForTesting(
            List<IbfCompareColumn> sourceColumns) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
