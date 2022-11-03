package com.example.core.mocks;

import com.example.core.ColumnType;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.example.core2.Output;
import com.example.snowflakecritic.SnowflakeConnectorState;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 4:19 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class MockOutput2<S> extends Output {
    private Map<TableRef, List<Map<String, Object>>> allAsMap = new HashMap<>();

    public MockOutput2(S state) {

    }

    public class Counter {
        long upsertCounter, updateCounter, deleteCounter;

        @Override
        public String toString() {
            return "upsertCounter="
                    + upsertCounter
                    + ", updateCounter="
                    + updateCounter
                    + ", deleteCounter="
                    + deleteCounter;
        }

        public long getUpsertCounter() {
            return upsertCounter;
        }

        public void setUpsertCounter(long upsertCounter) {
            this.upsertCounter = upsertCounter;
        }

        public long getUpdateCounter() {
            return updateCounter;
        }

        public void setUpdateCounter(long updateCounter) {
            this.updateCounter = updateCounter;
        }

        public long getDeleteCounter() {
            return deleteCounter;
        }

        public void setDeleteCounter(long deleteCounter) {
            this.deleteCounter = deleteCounter;
        }
    }

    long checkpointCounter;

    private Map<TableRef, MockOutput2.Counter> counters = new HashMap<>();

    //@Override
    public TableDefinition promoteColumnType(TableDefinition tableDefinition, String columnName, ColumnType newType) {
        return tableDefinition;
    }

    //@Override
    //public TableDefinition updateSchema(TableDefinition table, UpdateSchemaOperation updateSchema) {
    //    return table;
    //}

    //@Override
    public void upsert(TableDefinition table, Object value, OperationTag... tags) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).upsertCounter;
    }

    //@Override
    public void upsert(TableDefinition table, Object value, Instant operationTimestamp, OperationTag... tags) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).upsertCounter;
    }

    //@Override
    public void update(TableDefinition table, Object diff, OperationTag... tags) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).updateCounter;
    }

    //@Override
    public void update(TableDefinition table, Object diff, Instant operationTimestamp, OperationTag... tags) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).updateCounter;
    }

    //@Override
    public void delete(TableDefinition table, Object deleteKeys) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).deleteCounter;
    }

    //@Override
    public void delete(TableDefinition table, Object deleteKeys, Instant operationTimestamp) {
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new MockOutput2.Counter()).deleteCounter;
    }

    @Override
    public void delete(TableDefinition tableDefinition, Map values) {
        ++counters.computeIfAbsent(tableDefinition.tableRef, tableRef -> new MockOutput2.Counter()).deleteCounter;
    }

    //@Override
    public void hardDelete(TableRef tableRef, String deletedColumn, Instant deleteBefore, boolean historyMode) {}

    //@Override
    public void softDelete(TableRef table, String setIsDeletedColumn, String whereTimeColumn, Instant syncBeginTime) {}

    //@Override
    public void assertTableSchema(TableDefinition tableDef) {}

//    @Override
    public void checkpoint(SnowflakeConnectorState snowflakeConnectorState) {
        ++checkpointCounter;
    }

    public Counter getCounter(TableRef tableRef) {
        return counters.get(tableRef);
    }

    //@Override
    //public void signal(SignalType signalType, TableRef... tableRefs) {}

    //@Override
    //public RenamingFilter2 renamingFilter() {
    //    return null;
    //}

    //@Override
    //public void warn(Warning warning) {}

    //@Override
    public void close() {

        System.out.println("");
        System.out.println("Output Stats");

        for (Map.Entry<TableRef, MockOutput2.Counter> entry : counters.entrySet()) {
            System.out.println("   " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("   checkpointCounter=" + checkpointCounter);
        System.out.println("");
    }
}