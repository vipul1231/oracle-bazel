package com.example.snowflakecritic;

import com.example.db.SyncerCommon;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class ResultSetHelper {
    private final ResultSet rows;
    private final SnowflakeTableInfo tableInfo;

    public ResultSetHelper(SnowflakeTableInfo tableInfo, ResultSet rows) {
        this.tableInfo = tableInfo;
        this.rows = rows;
    }

    public Map<String, Object> extractRowValues() throws SQLException {

        Map<String, Object> values = new HashMap<>();
        for (SnowflakeColumnInfo columnInfo : tableInfo.includedColumnInfo()) {
            values.put(columnInfo.columnName, getColumnValue(columnInfo));
        }

        return values;
    }

    private LocalDate toLocalDate(Date sqlDate) {
        return sqlDate.toLocalDate();
    }

    public Object getColumnValue(SnowflakeColumnInfo columnInfo) throws SQLException {
        switch (columnInfo.sourceType) {
            case NUMBER:
            case FLOAT:
                return rows.getBigDecimal(columnInfo.columnName);
            case TIME:
            case TEXT:
                return rows.getString(columnInfo.columnName);
            case TIMESTAMP_TZ:
            case TIMESTAMP_LTZ:
                return SyncerCommon.convertColumnValue(rows.getTimestamp(columnInfo.columnName), Timestamp::toInstant);
            case TIMESTAMP_NTZ:
                return SyncerCommon.convertColumnValue(
                        rows.getTimestamp(columnInfo.columnName), Timestamp::toLocalDateTime);
            case DATE:
                return SyncerCommon.convertColumnValue(rows.getDate(columnInfo.columnName), this::toLocalDate);
            case BINARY:
                return rows.getBytes(columnInfo.columnName);
            case BOOLEAN:
                return rows.getBoolean(columnInfo.columnName);
            default:
                throw new IllegalArgumentException(String.format("Unsupported type %s", columnInfo.sourceType));
        }
    }
}
