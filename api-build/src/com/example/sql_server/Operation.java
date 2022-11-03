package com.example.sql_server;

/**
 * CDC operations are represented by numbers ranging from 1 to 4. more info:
 * https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql#table-returned
 */
enum Operation {
    CT_DELETE,
    CDC_DELETE,
    INSERT,
    OLD_UPDATE,
    UPDATE;

    static Operation fromCT(String op) {
        switch (op) {
            case "D":
                return CT_DELETE;
            case "I":
                return INSERT;
            case "U":
                return UPDATE;
            default:
                throw new IllegalArgumentException(
                        "Change tracking operation must be of the following characters: 'D', 'I', 'U'. Actual value is: "
                                + op);
        }
    }

    static Operation fromCDC(int op) {
        switch (op) {
            case 1:
                return CDC_DELETE;
            case 2:
                return INSERT;
            case 3:
                return OLD_UPDATE;
            case 4:
                return UPDATE;
            default:
                throw new IllegalArgumentException("CDC operation must be between 1 and 4, actual value is: " + op);
        }
    }
}
