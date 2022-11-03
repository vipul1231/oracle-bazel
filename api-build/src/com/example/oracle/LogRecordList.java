package com.example.oracle;

import javax.transaction.xa.Xid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LogRecordList implements Iterable {

    private List<CustomLogRecord> logRecordList;

    public LogRecordList() {
        logRecordList = new ArrayList<>();
    }

    public LogRecordList(List<CustomLogRecord> logRecordList) {
        this.logRecordList = logRecordList;
    }

    public List<CustomLogRecord> getLogRecordList() {
        return logRecordList;
    }

    public void setLogRecordList(List<CustomLogRecord> logRecordList) {
        this.logRecordList = logRecordList;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public long getLastScn() {
        return 1;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    @Override
    public Iterator iterator() {
        return null;
    }

    /**
     * @TODO: Need to add business logic
     * @return
     */

    public Xid getXid() {
        return null;

    }

    /**@TODO: Need to write business logic
     *
     * @return
     */
    public Instant getCommitTime() {
        return null;
    }

    /**
     * @return
     * @TODO: Need to write business logic
     */
    public CustomLogRecord getFirstRecord() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     */
    public void freeCache() {
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public CustomLogRecord getNextRecord() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param timestamp
     */
    public void closeForWrite(Instant timestamp) {
    }
}
