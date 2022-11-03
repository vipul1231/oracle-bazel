package com.example.oracle;

import com.example.ibf.schema.IbfColumnInfo;

public class OracleIbfColumnInfo extends IbfColumnInfo {

    private final OracleColumnInfo oracleColumnInfo;

    /**
     * When we construct this object we don't know its default column value yet. It will be looked up later by the
     * OracleIbfSchemaChangeManager.
     */
    public OracleIbfColumnInfo(OracleColumnInfo oracleColumnInfo) {
        super(oracleColumnInfo.getName(), oracleColumnInfo.getDestinationType());
        this.oracleColumnInfo = oracleColumnInfo;
    }

    public OracleColumnInfo getOracleColumnInfo() {
        return oracleColumnInfo;
    }

    public void setDefaultValue(Object defaultValue) {
        this.columnDefaultValue = defaultValue;
    }

}
