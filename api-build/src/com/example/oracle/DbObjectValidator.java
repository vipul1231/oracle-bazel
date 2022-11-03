package com.example.oracle;

public interface DbObjectValidator {

    void validate(OracleTableInfo tableInfo) throws Exception;

    boolean columnTypeSupported(OracleType type);

    boolean primaryKeyTypeSupported(OracleType type);
}

