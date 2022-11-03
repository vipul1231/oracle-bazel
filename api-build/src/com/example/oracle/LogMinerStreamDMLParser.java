package com.example.oracle;

import com.example.core.TableRef;

import javax.transaction.xa.Xid;
import java.util.List;
import java.util.Map;

public class LogMinerStreamDMLParser {

    /**
     * @TODO: Need to write business logic
     * @param table
     * @param columns
     * @param outputHelper
     * @param oracleLogMinerValueConverter
     */
    public LogMinerStreamDMLParser(TableRef table,
                                   List<OracleColumn> columns,
                                   OracleOutputHelper outputHelper,
                                   OracleLogMinerValueConverter oracleLogMinerValueConverter) {
    }

    /**
     * @TODO: Need to write business logic
     * @param table
     * @param columns
     * @param outputHelper
     */
    public LogMinerStreamDMLParser(TableRef table, List<OracleColumn> columns, OracleOutputHelper outputHelper) {
    }

    /**
     * @TODO: Need to write business logic
     * @param newValues
     */
    public void convertStr2NativeType(Map<String, Object> newValues) {
    }

    /**
     * @TODO: Need to write business logic
     * @param sql
     * @param xid
     * @return
     */
    public Map<String, Object> getRow(String sql, Xid xid) {
        return null;
    }
}
