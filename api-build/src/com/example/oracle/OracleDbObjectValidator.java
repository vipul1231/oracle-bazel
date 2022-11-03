package com.example.oracle;


public class OracleDbObjectValidator implements DbObjectValidator {
    @Override
    public void validate(OracleTableInfo tableInfo) throws Exception {}

    @Override
    public boolean columnTypeSupported(OracleType type) {
        return type.getWarehouseType().isPresent();
    }

    @Override
    public boolean primaryKeyTypeSupported(OracleType type) {
        return true;
    }
}

