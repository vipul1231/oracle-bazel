package com.example.oracle;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 1:54 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

public enum OracleErrorCode {
    // Ordinal order is not important: insert new entries in ascending error code order for readability.
    ORA_00904(904, "ORA-00904"), // invalid identifier
    ORA_01291(1291, "ORA-01291"),

    ORA_01284(1284, "ORA_01284"),

    ORA_01292(1292, "ORA_01292"),
    ORA_01466(1466, "ORA-01466"), // unable to read data - table definition has changed
    ORA_01555(1555, "ORA-01555"), // snapshot too old
    ORA_06564(6564, "ORA-06564"), // object X does not exist
    ORA_08181(8181, "ORA-08181"), // specified number is not a valid system change number
    ORA_30052(30052, "ORA-30052"),// invalid lower limit snapshot expression
    NUMERIC_OVERFLOW(0, "Numeric Overflow");

    ;

    private int errorCode;
    private String errorCodeLabel;

    OracleErrorCode(int code, String err) {
        this.errorCode = code;
        this.errorCodeLabel = err;
    }

    public boolean isCauseOf(Exception ex) {
        return true;
        //return is(ex.getCause());
    }


    public boolean is(SQLException ex) {
        return ex.getErrorCode() == errorCode || is(ex.getMessage());
    }

    public boolean is(String err) {
        return err != null && err.contains(errorCodeLabel);
    }
}