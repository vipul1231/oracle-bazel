package com.example.oracle;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:18 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public enum ChangeType {
    INSERT,
    UPDATE,
    DELETE;

    @SuppressWarnings("MethodWithMultipleReturnPoints")
    public static ChangeType changeType(LogMinerOperation op) {
        switch (op) {
            case INSERT_OP:
                return ChangeType.INSERT;
            case DELETE_OP:
                return ChangeType.DELETE;
            case UPDATE_OP:
                return ChangeType.UPDATE;
            default:
                throw new RuntimeException("Expected INSERT, UPDATE, DELETE but found " + op);
        }
    }

    @SuppressWarnings("MethodWithMultipleReturnPoints")
    public static ChangeType flashbackChangeType(String op) {
        switch (op) {
            case "I":
                return ChangeType.INSERT;
            case "D":
                return ChangeType.DELETE;
            case "U":
                return ChangeType.UPDATE;
            default:
                throw new RuntimeException("Expected INSERT, UPDATE, DELETE but found " + op);
        }
    }
}
