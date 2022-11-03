package com.example.snowflakecritic.scripts;

import com.example.core.ColumnType;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.warning.Warning;
import com.example.core2.OperationTag;
import com.example.core2.Output;
import com.example.snowflakecritic.SnowflakeConnectorState;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ScriptRunnerOutput extends Output<SnowflakeConnectorState> {

    class Counter {
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
    }

    long checkpointCounter;

    private Map<TableRef, Counter> counters = new HashMap<>();

    protected void write(TableRef tableRef, String message) {
        System.out.println(tableRef.toString() + ": " + message);
    }

    //@Override
    public TableDefinition promoteColumnType(TableDefinition tableDefinition, String columnName, ColumnType newType) {
        return tableDefinition;
    }

    //@Override
    //public TableDefinition updateSchema(TableDefinition table, UpdateSchemaOperation updateSchema) {
    //    return table;
    //}

    @Override
    public void upsert(TableDefinition table, Object value, OperationTag... tags) {
        write(table.tableRef, String.format("upsert: %s tags: %s", value, Arrays.toString(tags)));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).upsertCounter;
    }

    @Override
    public void upsert(TableDefinition table, Object value, Instant operationTimestamp, OperationTag... tags) {
        write(
                table.tableRef,
                String.format("upsert: %s opTimestamp: %s tags: %s", value, operationTimestamp, Arrays.toString(tags)));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).upsertCounter;
    }

    @Override
    public void update(TableDefinition table, Object diff, OperationTag... tags) {
        write(table.tableRef, String.format("update: %s tags: %s", diff, Arrays.toString(tags)));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).updateCounter;
    }

    @Override
    public void update(TableDefinition table, Object diff, Instant operationTimestamp, OperationTag... tags) {
        write(
                table.tableRef,
                String.format("update: %s opTimestamp: %s tags: %s", diff, operationTimestamp, Arrays.toString(tags)));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).updateCounter;
    }

    @Override
    public void delete(TableDefinition table, Object deleteKeys) {
        write(table.tableRef, String.format("delete keys %s", deleteKeys));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).deleteCounter;
    }

    @Override
    public void delete(TableDefinition table, Object deleteKeys, Instant operationTimestamp) {
        write(table.tableRef, String.format("delete: keys %s opTimestamp: %s ", deleteKeys, operationTimestamp));
        ++counters.computeIfAbsent(table.tableRef, tableRef -> new Counter()).deleteCounter;
    }

    @Override
    public void hardDelete(TableRef tableRef, String deletedColumn, Instant deleteBefore, boolean historyMode) {
        write(
                tableRef,
                String.format(
                        "hardDelete: column %s deleteBefore: %s historyMode: %s",
                        deletedColumn, deleteBefore, historyMode));
    }

    @Override
    public void softDelete(
            TableRef tableRef, String setIsDeletedColumn, String whereTimeColumn, Instant syncBeginTime) {
        write(
                tableRef,
                String.format(
                        "softDelete: %s %s syncBeginTime: %s", setIsDeletedColumn, whereTimeColumn, syncBeginTime));
    }

    //@Override
    public void assertTableSchema(TableDefinition tableDef) {}

    @Override
    public void checkpoint(SnowflakeConnectorState snowflakeConnectorState) {
        ++checkpointCounter;
    }

    //@Override
    //public void signal(SignalType signalType, TableRef... tableRefs) {}

    //@Override
    //public RenamingFilter2 renamingFilter() {
    //    return null;
    //}

    //@Override
    public void warn(Warning warning) {}

    @Override
    public void close() {

        System.out.println("");
        System.out.println("Output Stats");

        for (Map.Entry<TableRef, Counter> entry : counters.entrySet()) {
            System.out.println("   " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("   checkpointCounter=" + checkpointCounter);
        System.out.println("");
    }
}
