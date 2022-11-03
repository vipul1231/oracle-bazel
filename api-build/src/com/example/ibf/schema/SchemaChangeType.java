package com.example.ibf.schema;

public enum SchemaChangeType {
    ADD_COLUMN("New columns are added"),
    DROP_COLUMN("Columns are dropped"),
    RENAME_COLUMN("Columns are renamed"),
    CHANGE_COLUMN_TYPE("Column types are changed"),
    UNKNOWN_CHANGE("Unknown change"),
    NO_CHANGE("No change");

    String description;

    SchemaChangeType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
