package com.example.sql_server;

import com.example.core.TableRef;
import com.example.utils.ExampleClock;

import java.math.BigInteger;
import java.time.Instant;
import java.util.*;

import static java.util.stream.Collectors.toSet;

public class SqlServerState {

    public final Map<TableRef, TableState> tables = new HashMap<>();
    public Set<TableRef> importWithOrderByPk = new HashSet<>();
    private Map<TableRef, TridentCursor> readCursors = new HashMap<>();
    private String tridentStorageId = null;
    private byte[] encryptedKey = null;

    /**
     * temporary variable for test cases. If this is true, you will be checking with import start time
     * instead of import finish time
     */
    private boolean checkWithImportStartTime = false;

    public boolean isCheckWithImportStartTime() {
        return checkWithImportStartTime;
    }

    public void setCheckWithImportStartTime(boolean checkWithImportStartTime) {
        this.checkWithImportStartTime = checkWithImportStartTime;
    }

    public void setReadCursors(Map<TableRef, TridentCursor> readCursor) {
        this.readCursors = readCursor;
    }

    public Map<TableRef, TridentCursor> getReadCursors() {
        return readCursors;
    }

    public void clearReadCursorFor(TableRef table) {
        readCursors.remove(table);
    }

    public void clearReadCursors() {
        readCursors.clear();
    }

    public String getTridentStorageId() {
        if (tridentStorageId == null) {
            setTridentStorageId(UUID.randomUUID().toString());
        }

        return tridentStorageId;
    }

    public void setTridentStorageId(String newTridentStorageId) {
        this.tridentStorageId = newTridentStorageId;
    }

    public void setEncryptedKey(byte[] encryptedKey) {
        this.encryptedKey = encryptedKey;
    }

    public byte[] getEncryptedKey() {
        return encryptedKey;
    }

    public static class TableState {
        public Instant importBeginTime;
        public boolean importFinished;
        /**
         * import finished time
         */
        public Instant importFinishedTime;

        public boolean wasSoftDeleted;
        public Optional<Long> snapshot;
        public Optional<BigInteger> lsn;
        public Optional<List<String>> key;

        // jackson dependency
        public TableState() {
            this(null);
        }

        public TableState(Instant importBeginTime) {
            this.importBeginTime = importBeginTime;
            this.importFinished = false;
            this.wasSoftDeleted = false;
            this.snapshot = Optional.empty();
            this.lsn = Optional.empty();
            this.key = Optional.empty();
        }

        public void setKeyCursor(Optional<List<String>> key) {
            if (importFinished)
                throw new IllegalStateException("Table must be currently importing for the key cursor to be set");
            this.key = key;
        }

        public void setImportFinished() {
            this.key = Optional.empty();
            this.importFinished = true;
            this.importFinishedTime = Instant.now();
            System.out.println(">>>>>>>>>>>>>>> Set the last imported time for the table as :" + importFinishedTime);

        }

        public boolean importStarted() {
            return this.importFinished || this.key.isPresent() || importBeginTime != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableState that = (TableState) o;

            if (importFinished != that.importFinished) return false;
            if (!importBeginTime.equals(that.importBeginTime)) return false;
            if (!snapshot.equals(that.snapshot)) return false;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = importBeginTime.hashCode();
            result = 31 * result + (importFinished ? 1 : 0);
            result = 31 * result + snapshot.hashCode();
            result = 31 * result + key.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "TableState{"
                    + "importBeginTime="
                    + importBeginTime
                    + ", importFinished="
                    + importFinished
                    + ", importFinishedTime="
                    + importFinishedTime
                    + ", key="
                    + key
                    + '}';
        }
    }

    public Optional<Long> getSnapshot(TableRef table) {
        return hasTable(table) ? tables.get(table).snapshot : Optional.empty();
    }

    public Optional<BigInteger> getLsn(TableRef table) {
        return hasTable(table) ? tables.get(table).lsn : Optional.empty();
    }

    public void updateSnapshotV3(TableRef table, Optional<Long> snapshot) {
        tables.get(table).snapshot = snapshot;
    }

    public void updateLsn(TableRef table, Optional<BigInteger> lsn) {
        tables.get(table).lsn = lsn;
    }

    public void initTableState(TableRef table, Instant beginTime) {
        if (tables.get(table) != null) throw new RuntimeException("TableState already initialized for " + table);

        tables.put(table, new TableState(beginTime));
    }

    public void resetTableState(TableRef table) {
        validateImportBeginTimeIsSet(table);

        tables.put(table, new TableState(ExampleClock.Instant.now()));
    }

    public void setImportProgressV3(TableRef table, Optional<List<String>> keyCursor) {
        validateImportBeginTimeIsSet(table);

        TableState tableState = tables.get(table);
        tableState.setKeyCursor(keyCursor);
    }

    public void setImportFinishedV3(TableRef table) {
        validateImportBeginTimeIsSet(table);

        TableState tableState = tables.get(table);
        tableState.setImportFinished();
        System.out.println("SqlServerState :: setImportFinishedV3: Hashcode :" + hashCode() + " -> " + table + " -> " + tables.get(table));
    }

    public boolean isImportStarted(TableRef table) {
        return tables.containsKey(table) && tables.get(table).importStarted();
    }

    public boolean isImportFinished(TableRef table) {
        return tables.containsKey(table) && tables.get(table).importFinished;
    }

    public Instant getImportFinishedTime(TableRef table) {
        System.out.println("SqlServerState :: getImportFinishedTime: Hashcode :" + hashCode() + " Size -> " + tables.size() + " -> " + table + " - " + tables.get(table));
        return tables.containsKey(table) ? tables.get(table).importFinishedTime : null;
    }

    public Instant getImportBeginTime(TableRef table) {
        return tables.containsKey(table) ? tables.get(table).importBeginTime : null;
    }

    public boolean hasTable(TableRef table) {
        return tables.containsKey(table);
    }

    public Optional<List<String>> getLastKey(TableRef table) {
        return tables.get(table).key;
    }

    public void setTableWasSoftDeleted(TableRef table, boolean wasSoftDeleted) {
        tables.get(table).wasSoftDeleted = wasSoftDeleted;
    }

    private void validateImportBeginTimeIsSet(TableRef table) {
        if (tables.get(table) == null || tables.get(table).importBeginTime == null)
            throw new RuntimeException("Import begin has not been set");
    }

    /**
     * Clean up tables from state if they are no longer present in the source
     *
     * @param presentTables tables that are currently still present in the source
     */
    public void cleanUp(Set<TableRef> presentTables) {
        Set<TableRef> noLongerPresent =
                tables.keySet().stream().filter(table -> !presentTables.contains(table)).collect(toSet());
        noLongerPresent.forEach(tables::remove);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlServerState that = (SqlServerState) o;
        return tables.equals(that.tables)
//                && readCursors.equals(that.readCursors)
                && Objects.equals(tridentStorageId, that.tridentStorageId)
                && Arrays.equals(encryptedKey, that.encryptedKey)
                && importWithOrderByPk.equals(that.importWithOrderByPk);
    }

    @Override
    public int hashCode() {
//        int result = Objects.hash(tables, readCursors, tridentStorageId, importWithOrderByPk);
//        result = 31 * result + Arrays.hashCode(encryptedKey);
//        return result;
        return -1;
    }

    @Override
    public String toString() {
        return "SqlServerState{"
                + "tables="
                + tables
//                + ", readCursors="
//                + readCursors
                + ", tridentStorageId='"
                + tridentStorageId
                + '\''
                + ", encryptedKey="
                + Arrays.toString(encryptedKey)
                + ", importWithOrderByPk="
                + importWithOrderByPk
                + '}';
    }
}
