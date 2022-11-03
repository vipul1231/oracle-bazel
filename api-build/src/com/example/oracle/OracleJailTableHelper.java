package com.example.oracle;

import com.example.core.TableName;
import com.example.core.TableRef;

import java.util.Map;
import java.util.Optional;
import java.util.logging.LogRecord;

public class OracleJailTableHelper {

    private static TableRef tableref;

    /**
     * @TODO: Need to write business logic
     * @param logRecord
     * @return
     */
    public static Map<String, Object> buildRow(LogRecord logRecord) {
        return null;
    }

    public static TableRef getDestJailTableRef(Optional<String> schema) {
        return tableref;
    }
}
