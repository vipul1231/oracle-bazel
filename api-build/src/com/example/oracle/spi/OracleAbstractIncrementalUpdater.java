package com.example.oracle.spi;

import com.example.core.TableRef;
import com.example.oracle.ChangeType;
import com.example.oracle.OracleColumn;
import com.example.oracle.OracleLogStreamException;
import com.example.oracle.Transactions;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 9:58 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public interface OracleAbstractIncrementalUpdater extends AutoCloseable {
    void start();

    void resync(TableRef tableRef, String reason, Boolean logEventNow);

    void forEachChange(
            TableRef table,
            String rowId,
            Map<String, Object> row,
            ChangeType type,
            Instant timeOfChange,
            Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys,
            Map<TableRef, List<OracleColumn>> selected);

    /**
     * Call if all done syncing
     */
    void stop() throws OracleLogStreamException;

    @Override
    void close();

    /**
     * Perform some incremental updates
     */
    void doIncrementalWork() throws OracleLogStreamException;
}
