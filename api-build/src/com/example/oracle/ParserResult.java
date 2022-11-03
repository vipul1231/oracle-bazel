package com.example.oracle;

import java.util.List;
import java.util.Map;

public class ParserResult {

    private CustomLogRecord customLogRecord;

    /**
     * @TODO: Need to write business logic
     * @param rec
     * @param newValues
     * @param oldValues
     * @param pkeyList
     */
    public ParserResult(CustomLogRecord rec,
                        Map<String, Object> newValues,
                        Map<String, Object> oldValues,
                        List<OracleColumn> pkeyList) {
    }

    /**
     * @TODO: Need to write business logic
     * @param newRow
     * @return
     */
    public boolean compareNewValues(ParserResult newRow) {
        return Boolean.TRUE;
    }

    public CustomLogRecord getRecord() {
        return customLogRecord;
    }

    public void setRecord(CustomLogRecord record) {
        this.customLogRecord = record;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public List<OracleColumn> getPkeyList() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public Map<String, Object> getNewValues() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public Map<String, Object> getOldValues() {
        return null;
    }
}
