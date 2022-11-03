package com.example.sql_server;

import com.example.core.TableRef;

import java.util.*;

class SqlServerKey {
    final TableRef table;
    final Type type;
    final boolean clustered;
    final Optional<TableRef> referencedTable;
    final Map<String, Integer> columns = new HashMap<>();
    final Set<String> referenceColumns = new HashSet<>();

    SqlServerKey(TableRef table, String type, int indexId, Optional<TableRef> referencedTable) {
        this.table = table;
        this.type = Type.valueOf(type.toUpperCase());
        this.clustered = indexId == 1;
        this.referencedTable = referencedTable;
    }

    boolean isPrimaryKey() {
        return type == Type.PRIMARY;
    }

    void addColumn(String column, Integer ordinalPosition) {
        columns.put(column, ordinalPosition);
    }

    void addReferenceColumn(String column) {
        if (null != column) {
            referenceColumns.add(column);
        }
    }

    @Override
    public String toString() {
        return "Key{"
                + " table="
                + table
                + " type="
                + type
                + " clustered="
                + clustered
                + " referencedTable="
                + referencedTable
                + " keyColumns="
                + columns
                + " referenceColumns="
                + referenceColumns
                + " }";
    }

    enum Type {
        PRIMARY,
        UNIQUE,
        FOREIGN
    }
}
