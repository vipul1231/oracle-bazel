package com.example.oracle;

import com.example.core.TableRef;

import java.util.Formatter;

import static com.example.oracle.Constants.defaultRACBlockRange;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/8/2021<br/>
 * Time: 8:26 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class Util {
    public static String binaryXidToString(byte[] xid) {
        try (Formatter formatter = new Formatter()) {
            for (byte b : xid) {
                formatter.format("%02x", b);
            }
            return formatter.toString().toUpperCase();
        }
    }

    public static Constants.KeyType convertToKeyType(String type) {
        switch (type) {
            case "P":
                return Constants.KeyType.PRIMARY;
            case "R":
                return Constants.KeyType.FOREIGN;
            default:
                throw new RuntimeException("Invalid key type: " + type);
        }
    }

    public static String doubleQuote(TableRef table) {
        return doubleQuote(table.schema) + '.' + doubleQuote(table.name);
    }

    public static String doubleQuote(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    public static String singleQuote(String value) {
        return "'" + value + "'";
    }

    public static long calculateBlockSize(BlockRange range) {
        return Math.min(range.max - range.min, defaultRACBlockRange);
    }
}