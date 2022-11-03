package com.example.db;

import com.example.core.Column;
import com.example.core.TableRef;
import com.example.oracle.Names;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SyncerCommon {

//    private final HashIdGenerator hashIdGenerator;

//    public SyncerCommon() {
//        this.hashIdGenerator = HashIdGenerator.create().build();
//    }

    public String calculateHashId(Collection<Column> sourceColumns, TableRef tableRef, Map<String, Object> values) {
        Map<String, Object> valuesToHash = new HashMap<>();

        for (Column column : sourceColumns) valuesToHash.put(column.name, values.get(column.name));

//        return hashIdGenerator.generate(valuesToHash, tableRef, new ArrayList<>(sourceColumns));
        return null;
    }

    public static <T, V> V convertColumnValue(T columnValue, Function<T, V> converter) {
        return columnValue == null ? null : converter.apply(columnValue);
    }

    public boolean addexampleIdColumn(com.example.db.DbTableInfo<?> tableInfo, Map<String, Object> values) {
        if (!tableInfo.hasPrimaryKey()) {
            String hashId = calculateHashId(tableInfo.sourceColumns(), tableInfo.sourceTable, values);
            values.put(Names.example_ID_COLUMN, hashId);
            return true;
        }

        return false;
    }
}
