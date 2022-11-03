package com.example.oracle;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:20 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public enum RowIdType {
    RESTRICTED(0),
    EXTENDED(1);

    public final int code;
    private static final Map<Integer, RowIdType> map = new HashMap<>();

    RowIdType(int code) {
        this.code = code;
    }

    static {
        for (RowIdType op : RowIdType.values()) {
            map.put(op.code, op);
        }
    }

    public static RowIdType valueOf(int op) {
        return map.get(op);
    }
}