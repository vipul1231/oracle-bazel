package com.example.oracle;

import com.example.core.TableName;
import com.example.core.TableRef;
import net.snowflake.client.jdbc.internal.joda.time.Hours;

import javax.transaction.xa.Xid;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class CustomLogRecord extends LogRecord {

    //@TODO: need to have right type of cache
    public Object cached;
    private TableName tableName;

    private TableRef tableRef;

    private Xid xid;

    private Duration duration;

    // @TODO: Need to set some default value
    private static final Level default_level = Level.WARNING;

    //@TODO: Need to set some default message
    private static final String default_msg = "some default message";

    /**
     * @TODO: What is this element. Do it belongs to some library or some custom element Set its correct Type
     */
    private CustomElement element;
    private LogMinerOperation operation;

    private RollbackInfo rollbackInfo;

    public CustomLogRecord(Level level, String msg) {
        super(level, msg);
    }

    /**
     * @TODO: Need to write business logic
     * @param xid
     */
    public CustomLogRecord(Xid xid) {
        super(default_level, default_msg);
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public long getScn() {
        return 1;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public Instant getTimestamp() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public TableRef getTable() {
        return this.tableRef;
    }

    public Optional<String> getSchema() {
        return this.tableName.schema;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public Xid getXid() {
        return this.xid;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public String getRowId() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public CustomElement getElement() {
        return this.element;
    }

    public void setElement(CustomElement element) {
        this.element = element;
    }

    /**
     * @TODO: Need to write business logic for same
     * @return
     */
    public LogMinerOperation getOperation() {
        return this.operation;
    }

    public void setOperation(LogMinerOperation operation) {
        this.operation = operation;
    }

    /**
     * @TODO: Need to write business logic
     * @param timestampFromScn
     */
    public void setTimestamp(Instant timestampFromScn) {
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
    public RollbackInfo getRollbackInfo() {
        return rollbackInfo;
    }

    public void setRollbackInfo(RollbackInfo rollbackInfo) {
        this.rollbackInfo = rollbackInfo;
    }

    /**
     * @TODO: Need to write business logic
     * @param rowId
     */
    public void setRowId(String rowId) {
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public String getRawData() {
        return "";
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public void setScn(long aLong) {
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public void setTable(TableRef table) {
    }

    /**
     * @TODO: Need to write business logic
     * @param data
     */
    public void setRawData(String data) {
    }
}
