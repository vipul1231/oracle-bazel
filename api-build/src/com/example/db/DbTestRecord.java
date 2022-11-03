package com.example.db;

import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.oracle.Record;

import java.util.Map;

public class DbTestRecord {
    public TableDefinition tableDefinition;
    public final Map<String, Object> record;

    public DbTestRecord(TableDefinition tableDefinition, Map<String, Object> record) {
        this.tableDefinition = tableDefinition;
        this.record = record;
    }

    public DbTestRecord(TableDefinition tableDefinition, Object record) {
        this.tableDefinition = tableDefinition;
        assert record instanceof Map : "Expected Map output, but found " + record.getClass();
        this.record = (Map) record;
    }

    public DbTestRecord(Record record) {
        this.record = record.toMap();

        TableRef tableRef = null;
//        TableRef tableRef =
//                record.type.name.schema.isPresent()
//                        ? new TableRef(record.type.name.schema.get(), record.type.name.table)
//                        : TableRef.fromTableName(record.type.name);

//        this.tableDefinition = TableDefinition.prepareTableDefinition(tableRef, record.knownTypes).build();
    }
}
