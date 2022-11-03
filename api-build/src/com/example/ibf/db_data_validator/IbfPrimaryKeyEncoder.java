package com.example.ibf.db_data_validator;

import com.example.core.annotations.DataType;
import com.example.ibf.IbfTableInspector;
import com.example.snowflakecritic.ibf.StrataEstimator;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IbfPrimaryKeyEncoder extends IbfTableInspector {
    /**
     * Get key lengths *
     */
    List<Integer> keyLengths();

    /**
     * Get data types of primary keys *
     */
    List<DataType> keyTypes();

    /**
     * Get primary key names *
     */
    List<String> getKeyNames();

    /**
     * Get all column names *
     */
    Set<String> getColumns();

    /**
     * Fetch a StrataEstimator representing a database table's primary key column(s) from the database by running a
     * query
     *
     * <p>See com.example.ibf.StrataEstimator for more details on this data structure
     *
     * @return StrataEstimator
     * @throws SQLException
     */
    StrataEstimator getPrimaryKeyStrataEstimator() throws SQLException;

    /**
     * Retrieve data for the given columns with the given primary keys
     *
     * @param primaryKeyValues a set of primary key values that we want to retrieve
     * @param columns          a list of columns that we want to retrieve
     * @return a map of map that consists of column names and values
     * @throws SQLException
     */
    Map<String, Map<String, Object>> retrieveRows(Set<List<Object>> primaryKeyValues, Set<String> columns)
            throws SQLException;
}
