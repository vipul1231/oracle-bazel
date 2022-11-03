package com.example.db;

public interface SchemaNameMapper {

    String toOutputSchema(String inputSchema);

    static SchemaNameMapper defaultMapper(String connectionName) {
        return (inputSchema) -> connectionName + "_" + inputSchema;
    }
}
