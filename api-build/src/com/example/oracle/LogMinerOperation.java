package com.example.oracle;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/28/2021<br/>
 * Time: 11:50 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public enum LogMinerOperation {
    INSERT_OP(1),
    DELETE_OP(2),
    UPDATE_OP(3),
    DDL_OP(5),
    START_XID(6),
    COMMIT_XID(7),
    ROLLBACK_XID(36),
    INCOMPLETE_XID(-1),

    //@TODO: Need to set proper value as per business logic
    UNSUPPORTED(-10);

    public final int code;
    private static final Map<Integer, LogMinerOperation> map = new HashMap<>();

    LogMinerOperation(int code) {
        this.code = code;
    }

    static {
        for (LogMinerOperation op : LogMinerOperation.values()) {
            map.put(op.code, op);
        }
    }

    public static LogMinerOperation valueOf(int op) {
        return map.get(op);
    }
}