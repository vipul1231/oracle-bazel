package com.example.oracle;

import com.example.core.TableRef;

import java.sql.SQLException;
import java.util.Formatter;
import java.util.List;
import java.util.regex.Pattern;

public class RowUtils {
    /*
     * ROWIDs are 18-characters long. Based on observation, rowids ending with
     * 12 or more 'A's are invalid. Rowids composed of all 'A's are also invalid.
     */
    private static final Pattern INVALID_ROWID = Pattern.compile(".*(?:[A]{12,})$");
    static final String SKIP_VALUE = "skip-9Qq2Fa";

    public static boolean isInvalidRowId(String rowId) {
        return INVALID_ROWID.matcher(rowId).matches();
    }

    public static RowMap buildRow(List<OracleColumn> columns, Transactions.RowValues rowValues) throws SQLException {
        String[] keys = new String[columns.size()], values = new String[columns.size()];
        boolean[] present = new boolean[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            String colName = "c" + i;
            Object rawValue = rowValues.get(colName);
            String stringValue = (rawValue != null) ? rawValue.toString() : null;
            OracleColumn col = columns.get(i);
            keys[i] = col.name;

            if (stringValue != null) {
                // Originally, we trimmed whitespace on all values. With this flag, we trim only non-String-like values.
                if (!col.oracleType.isStringLike()) {
                    stringValue = stringValue.trim();
                }
                if (stringValue.equals(SKIP_VALUE)) {
                    continue;
                }
            }
            values[i] = stringValue;
            present[i] = true;
        }

        return new RowMap(keys, values, present);
    }

    public static String binaryXidToString(byte[] xid) {
        try (Formatter formatter = new Formatter()) {
            for (byte b : xid) {
                formatter.format("%02x", b);
            }
            return formatter.toString().toUpperCase();
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
}
