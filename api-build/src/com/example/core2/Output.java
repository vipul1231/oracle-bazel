package com.example.core2;

import com.example.core.ColumnType;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.warning.Warning;

import java.time.Instant;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 8:29 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class Output<T> implements AutoCloseable {
    public void warn(Warning warning) {

    }

    public void checkpoint(T state) {

    }

    public T getLastFlushedTime() {
        return null;
    }

    public void update(TableDefinition table, Map<String, Object> rowValues) {

    }

    public void delete(TableDefinition table, Map<String, Object> rowValues, Instant opTime) {

    }




    public void softDelete(TableRef destinationTableRef, String example_deleted_column, String example_synced_column, Instant importBeginTime) {

    }

    public void delete(TableDefinition tableDefinition, Map<String, Object> values) {

    }

    @Override
    public void close() throws Exception {

    }

    public void upsert(TableDefinition table, Object value, OperationTag... tags) {};

    /**
     * Insert a new row for records with different operation timestamp for history mode sync.
     *
     * <p>All the records prior to this should be marked as invalid.
     */
    public void upsert(TableDefinition table, Object value, Instant operationTimestamp, OperationTag... tags) {};

    /** Update an existing value in the data warehouse */
    public void update(TableDefinition table, Object diff, OperationTag... tags) {};

    /** Update an existing value in the data warehouse for history mode */
    public void update(TableDefinition table, Object diff, Instant operationTimestamp, OperationTag... tags){};

    /** Delete an existing value in the data warehouse */
    public void delete(TableDefinition table, Object deleteKeys){};

    /** Invalidate an existing value in the data warehouse in history mode sync */
    public void delete(TableDefinition table, Object deleteKeys, Instant operationTimestamp) {};

    /** Clear the existing data of the table in the data warehouse */
    public void hardDelete(TableDefinition tableDefinition, Instant deleteBefore) {
        hardDelete(
                tableDefinition.tableRef,
                tableDefinition.deletedColumn,
                deleteBefore,
                tableDefinition.isHistoryModeWrite);
    }

    /**
     * Clears the existing data of the table in the data warehouse.
     *
     * <p>It's highly recommended to use {@link #hardDelete(TableDefinition, Instant)} overload instead of this one when
     * you already have a {@link TableDefinition} instance.
     */
    public void hardDelete(
            TableRef tableRef,
            String deletedColumn,
            Instant deleteBefore,
            boolean historyMode) {

    }

    public TableDefinition promoteColumnType(
            TableDefinition tableDefinition, String columnName, ColumnType newType) {
        return new TableDefinition();
    };

}