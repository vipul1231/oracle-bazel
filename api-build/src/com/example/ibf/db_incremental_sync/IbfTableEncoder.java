package com.example.ibf.db_incremental_sync;

import com.example.core.annotations.DataType;
import com.example.ibf.IbfTableInspector;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.snowflakecritic.ibf.InvertibleBloomFilter;
import com.google.common.annotations.VisibleForTesting;

import java.sql.SQLException;

/**
 * Use IbfTableEncoderWithCompoundPK for new implementations
 */
public interface IbfTableEncoder extends IbfTableInspector {
    int DEFAULT_KEY_LENGTH = 1;

    /**
     * Fetch a ResizableInvertibleBloomFilter representing a database table from the database by running a query
     *
     * <p>See ResizableInvertibleBloomFilter for more details on this data structure
     *
     * @param smallCellCount
     * @param size
     * @return ResizableInvertibleBloomFilter
     * @throws SQLException
     */
    ResizableInvertibleBloomFilter getResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException;

    /**
     * Fetch a ResizableInvertibleBloomFilter to replace the current persisted InvertibleBloomFilter
     *
     * <p>See ResizableInvertibleBloomFilter for more details on this data structure
     *
     * @param smallCellCount
     * @param size
     * @return ResizableInvertibleBloomFilter
     * @throws SQLException
     */
    ResizableInvertibleBloomFilter getReplacementResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException;

    /**
     * Fetch an InvertibleBloomFilter representing a database table from the database by running a query
     *
     * <p>See InvertibleBloomFilter for more details on this data structure
     *
     * <p>The standard InvertibleBloomFilter is useful for testing purposes, but not used in production for the syncing
     * process
     *
     * @param cellCount
     * @return InvertibleBloomFilter
     * @throws SQLException
     */
    @VisibleForTesting
    InvertibleBloomFilter getInvertibleBloomFilter(int cellCount) throws SQLException;

    /** get key length * */
    @Deprecated
    default int keyLength() {
        throw new UnsupportedOperationException();
    }

    /** get data type of primary key * */
    @Deprecated
    default DataType keyType() {
        throw new UnsupportedOperationException();
    }

    /** check if new columns are added */
    boolean ifReplacementRequired();

    /** check if compound key is supported * */
    default boolean hasCompoundPkSupport() {
        return false;
    }
}

