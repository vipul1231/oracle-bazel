package com.example.core;


import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public class TableRef implements Comparable<TableRef>, Serializable {
    private static final long serialVersionUID = 6406553422917956411L;

    private final int hashCode;

    public final String schema, name;

    public TableRef(String schema, String name) {
        assert schema != null;
        assert name != null;
        this.schema = schema;
        this.name = name;
        this.hashCode = calculateHashCode();
    }

//    public static TableRef fromTableName(TableName newTable) {
//        return null;
//    }



    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public static TableRef parse(String s) {
        return null;
    }



    public String toLongString() {
        return schema + "." + name;
    }

    @Override
    public int compareTo(TableRef t) {
        return Comparator.comparing((TableRef table) -> table.schema)
                .thenComparing((TableRef table) -> table.name)
                .compare(this, t);
    }

    @Override
    public boolean equals(Object o) {
        return (this == o)
                || ((o != null)
                && (o.getClass() == getClass())
                && name.equals(((TableRef) o).name)
                && schema.equals(((TableRef) o).schema));
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int calculateHashCode() {
        int result = schema.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return escapePeriods(schema) + '.' + escapePeriods(name);
    }

    String escapePeriods(String name) {
        return name.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.");
    }

    public TableName toTableName() {
        return TableName.withSchema(schema, name);
    }

    /** @deprecated use {@link com.example.integrations.db.SchemaNameMapper} instead */
    @Deprecated
    public TableName toTableName(String schemaPrefix) {
        return TableName.withSchema(schemaPrefix + "_" + schema, name);
    }

    public static TableRef fromTableName(TableName table) {
        if (!table.schema.isPresent()) throw new RuntimeException("Schema name is required");

        return new TableRef(table.schema.get(), table.table);
    }
}

