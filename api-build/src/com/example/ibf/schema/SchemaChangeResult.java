package com.example.ibf.schema;

import java.util.Map;

public class SchemaChangeResult {
    public final SchemaChangeType type;
    public final Map<String, IbfColumnInfo> modifiedColumns;
    public final boolean isResyncNeeded;

    public SchemaChangeResult(SchemaChangeType type, Map<String, IbfColumnInfo> modifiedColumns) {
        this(type, modifiedColumns, true);
    }

    public SchemaChangeResult(
            SchemaChangeType type, Map<String, IbfColumnInfo> modifiedColumns, boolean isResyncNeeded) {
        this.type = type;
        this.modifiedColumns = modifiedColumns;
        this.isResyncNeeded = isResyncNeeded;
    }
}
