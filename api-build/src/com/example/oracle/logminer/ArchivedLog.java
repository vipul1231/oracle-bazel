package com.example.oracle.logminer;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:28 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Represents the Oracle entity/table V$ARCHIVED_LOG/SYS.V_$ARCHIVED_LOG
 */
public class ArchivedLog {

    /**
     * The columns of SYS.V_$ARCHIVED_LOG and their equivalent Java types
     */
    public enum ColumnDef {
        RECID(Long.class),
        STAMP(Long.class),
        NAME(String.class),
        DEST_ID(Long.class),
        THREAD_NUM("THREAD#", Long.class),
        SEQUENCE_NUM("SEQUENCE#", Long.class),
        RESETLOGS_CHANGE_NUM("RESETLOGS_CHANGE#", Long.class),
        RESETLOGS_TIME(java.sql.Date.class),
        RESETLOGS_ID(Long.class),
        /**
         * The lowest SCN number within the given archive. Represents the lower bound (inclusive) SCN of transactions
         * recorded in the given archive file.
         */
        FIRST_CHANGE_NUM("FIRST_CHANGE#", Long.class),
        FIRST_TIME(java.sql.Date.class),
        /**
         * NEXT_CHANGE# is like a pointer to the next block of SCN values. It represents the upperbound (non-inclusive)
         * SCN for the given archive file. Thus the range of covered SCNs in a given archive is
         * [FIRST_CHANGE#,NEXT_CHANGE#).
         */
        NEXT_CHANGE_NUM("NEXT_CHANGE#", Long.class),
        NEXT_TIME(java.sql.Date.class),
        BLOCKS(Long.class),
        BLOCK_SIZE(Long.class),
        CREATOR(String.class),
        REGISTRAR(String.class),
        STANDBY_DEST(Boolean.class),
        ARCHIVED(Boolean.class),
        APPLIED(Boolean.class),
        DELETED(Boolean.class),
        STATUS(String.class),
        COMPLETION_TIME(java.sql.Date.class),
        DICTIONARY_BEGIN(Boolean.class),
        DICTIONARY_END(Boolean.class),
        END_OF_REDO(java.sql.Date.class), // 2020-11-16 20:41:20
        BACKUP_COUNT(Long.class),
        ARCHIVAL_THREAD_NUM("ARCHIVAL_THREAD#", Long.class),
        ACTIVATION_NUM("ACTIVATION#", Long.class),
        IS_RECOVERY_DEST_FILE(Boolean.class),
        COMPRESSED(Boolean.class),
        FAL(Boolean.class),
        END_OF_REDO_TYPE(String.class),
        BACKED_BY_VSS(Boolean.class),
        CON_ID(Long.class),

        // Derived columns
        MIN_FIRST_CHANGE_NUM(Long.class),
        MAX_NEXT_CHANGE_NUM(Long.class),
        MIN_FIRST_TIME(java.sql.Date.class),
        MAX_NEXT_TIME(java.sql.Date.class);

        private String colName;
        private Class<?> dataType;
        private boolean useAlias;

        ColumnDef(Class<?> dataType) {
            this.colName = name();
            this.dataType = dataType;
        }

        ColumnDef(String colname, Class<?> dataType) {
            this.colName = colname;
            this.dataType = dataType;
            this.useAlias = true;
        }

        @Override
        public String toString() {
            return colName;
        }

        public String as() {
            return colName + (useAlias ? " AS " + name() : "");
        }

        public String getColName() {
            return colName;
        }

        public String getString(ResultSet rs) throws SQLException {
            return rs.getString(colName);
        }

        public Long getLong(ResultSet rs) throws SQLException {
            return rs.getLong(colName);
        }

        public Timestamp getTimestamp(ResultSet rs) throws SQLException {
            return rs.getTimestamp(colName);
        }
    }

    private String name;
    private Long nextChangeNum;
    private Long firstChangeNum;
    private LocalDateTime firstTime;
    private LocalDateTime nextTime;

    public LocalDateTime getFirstTime() {
        return firstTime;
    }

    public LocalDateTime getNextTime() {
        return nextTime;
    }

    public Long getSequenceNum() {
        return sequenceNum;
    }

    private Long sequenceNum;

    public String getName() {
        return name;
    }

    public Long getNextChangeNum() {
        return nextChangeNum;
    }

    public Long getFirstChangeNum() {
        return firstChangeNum;
    }

    @Override
    public String toString() {
        return "ArchivedLog{"
                + "name='"
                + name
                + '\''
                + ", firstChangeNum="
                + firstChangeNum
                + ", firstTime="
                + firstTime
                + ", nextChangeNum="
                + nextChangeNum
                + ", nextTime="
                + nextTime
                + ", sequenceNum="
                + sequenceNum
                + '}';
    }

    public static ArchivedLog from(ResultSet resultSet) throws SQLException {
        ArchivedLog archivedLog = new ArchivedLog();
        archivedLog.name = ColumnDef.NAME.getString(resultSet);
        archivedLog.nextChangeNum = ColumnDef.NEXT_CHANGE_NUM.getLong(resultSet);
        archivedLog.firstChangeNum = ColumnDef.FIRST_CHANGE_NUM.getLong(resultSet);
        archivedLog.sequenceNum = ColumnDef.SEQUENCE_NUM.getLong(resultSet);
        archivedLog.firstTime = asLocalDateTime(ColumnDef.FIRST_TIME.getTimestamp(resultSet));
        archivedLog.nextTime = asLocalDateTime(ColumnDef.NEXT_TIME.getTimestamp(resultSet));
        return archivedLog;
    }

    public static LocalDateTime asLocalDateTime(Timestamp timestamp) {
        if (null == timestamp) {
            return null;
        }

        return timestamp.toLocalDateTime();
    }
}