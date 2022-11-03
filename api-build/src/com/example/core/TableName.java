package com.example.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import java.util.function.Function;

public class TableName implements Comparable<TableName> {
    public final Namespace namespace;
    public final Optional<String> schema;
    public final String table;

    public TableName(
            @JsonProperty("namespace") Namespace namespace,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") String table) {
        this.namespace = namespace;
        this.schema = schema;
        this.table = table;

        if (namespace == Namespace.Warehouse && !schema.isPresent())
            throw new IllegalArgumentException("schema is required in Warehouse namespace");
    }

    public TableName(Optional<String> schema, String table) {
        this(Namespace.Source, schema, table);
    }

    public static TableName withSchema(String schema, String table) {
        return new TableName(Optional.of(schema), table);
    }

    public static TableName withoutSchema(String table) {
        return new TableName(Optional.empty(), table);
    }

    public String quotedName(Function<String, String> quote) {
        String quotedName = "";
        if (schema.isPresent()) quotedName += quote.apply(schema.get()) + ".";
        quotedName += quote.apply(table);
        return quotedName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableName tableName = (TableName) o;
        return namespace == tableName.namespace
                && java.util.Objects.equals(schema, tableName.schema)
                && java.util.Objects.equals(table, tableName.table);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(namespace, schema, table);
    }

    @Override
    public String toString() {
        return schema.orElse("?") + "." + table;
    }

    @Override
    public int compareTo(TableName that) {
        int compareSchemas = this.schema.orElse("").compareTo(that.schema.orElse(""));

        if (compareSchemas != 0) return compareSchemas;

        return this.table.compareTo(that.table);
    }
}
