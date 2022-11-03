package com.example.oracle;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class LogEntryBuilder {
    private long scn;
    private int op;
    private String xid;
    private String rowId;
    private int status = 0;
    private int rollback = 0;
    private String ddl = "FAKE DDL STATEMENT";

    public LogEntryBuilder(long scn, int op, String xid, String rowId) {
        this.scn = scn;
        this.op = op;
        this.xid = xid;
        this.rowId = rowId;
    }

    public LogEntryBuilder addStatus(int status) {
        this.status = status;
        return this;
    }

    public LogEntryBuilder addRollback(int rollback) {
        this.rollback = rollback;
        return this;
    }

    public LogEntryBuilder addDdl(String ddl) {
        this.ddl = ddl;
        return this;
    }

    private byte[] xidStringToBytes(String xid) {
        String[] splitXid = xid.split("(?<=\\G..)");
        byte[] xidBytes = new byte[splitXid.length];
        for (int i = 0; i < splitXid.length; i++) {
            xidBytes[i] = (byte) Integer.parseInt(splitXid[i], 16);
        }

        return xidBytes;
    }

    public Map<String, Object> build() {
        Map<String, Object> row = new HashMap<>();

        row.put("XID", xidStringToBytes(xid));
        row.put("SCN", new BigDecimal(scn));
        row.put("SEG_OWNER", "S");
        row.put("TABLE_NAME", "T");
        row.put("OPERATION_CODE", new BigDecimal(op));
        row.put("STATUS", status);
        row.put("ROW_ID", rowId);
        row.put("SCN_TIMESTAMP", Instant.now());
        row.put("INFO", "FAKE INFO");
        row.put("DDL_EVENT", ddl);
        row.put("ROLLBACK", rollback);

        return row;
    }
}
