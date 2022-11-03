package com.example.crypto;


import com.example.oracle.CustomLogRecord;
import com.example.oracle.LogRecordConverter;
import com.example.oracle.LogRecordList;
import com.example.oracle.SyncAdapter;

import javax.crypto.SecretKey;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogRecord;

public class OracleRecordQueue {
    private final String queueId = UUID.randomUUID().toString();
    Object defaultCursor;
    ByteArrayListOnFile logEventList;
    public LinkedHashMap<Xid, LogRecordList> xidMap = new LinkedHashMap<>();
//  LogRecordConverter logRecordConverter;
//  private SyncAdapter syncAdapter;
    public long totalWriteBytes = 0;
    public long totalWriteRecords = 0;

    private AtomicLong nCachedRecords = new AtomicLong(0);
    private volatile long nCachedXid = 0;
    public static final int PER_XID_MEM_USAGE = 256; // rough estimation
    public static final int PER_LOG_MEM_USAGE = 84; // rough estimation
    private SyncAdapter syncAdapter;
    private CustomLogRecord nextLogRecord;
    private CustomLogRecord lastLogRecord;

    public OracleRecordQueue(/*LogRecordConverter recordConverter*/) {
//        logRecordConverter = recordConverter;
    }

    public OracleRecordQueue(LogRecordConverter recordConverter) {
//        logRecordConverter = recordConverter;
    }

    public String encryptAndDecryptWithByteArrayListOnFile(String text) {
        SecretKey secretKey = Encrypt.newEphemeralKey();

        try {
            ByteArrayListOnFile byteArrayListOnFile = new ByteArrayListOnFile("/temp/", 1, new BasicCipherAdapter(secretKey));

            System.out.println("Encrypting the text :" + text);
            byte[] encrypted = byteArrayListOnFile.encrypt(text.getBytes());
            ElementImpl decrypted = byteArrayListOnFile.decrypt(new ElementImpl(encrypted));

            return new String(decrypted.getData(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public String encryptAndDecrypt(String text) {
        SecretKey secretKey = Encrypt.newEphemeralKey();

        BasicCipherAdapter basicCipherAdapter = new BasicCipherAdapter(secretKey);
        try {
            System.out.println("Encrypting the text :" + text);
            byte[] encrypted = basicCipherAdapter.encrypt(text.getBytes());
            byte[] decrypted = basicCipherAdapter.decrypt(encrypted);

            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void start() {
        try {
            SecretKey secretKey = Encrypt.newEphemeralKey();
            logEventList =
                    new ByteArrayListOnFile(
                            "/tmp/" + queueId, -1,
                            secretKey == null ? null : new BasicCipherAdapter(secretKey));

            defaultCursor = logEventList.createCursor();
            logEventList.rewind(defaultCursor);
        } catch (IOException e) {
//            throw new OracleRecordQueueException("Failed to create ByteArrayListOnFile.", e);
        }
    }


    /**
     * Add a record to xidMap.
     *
     * @param rec record to add to xidMap
     */
    public void addToTxQueue(LogRecord rec) {
//        synchronized (xidMap) {
//            LogRecordList records = xidMap.get(rec.getXid());
//            if (records == null) {
//                records = new LogRecordList(rec.getXid());
//                xidMap.put(rec.getXid(), records);
//                nCachedXid = nCachedXid + 1;
//            }
//            addToXidList(records, rec);
//        }
    }

    /**
     * Add the given log record in logEventList (ByteArrayList).
     *
     * @param rec record to add to logEventList
     */
    public void addToLogEventList(LogRecord rec) {
        try {
//            byte[] data = logRecordConverter.toByteArray(rec);
//            Element el = logEventList.append(data);
//            totalWriteBytes += data.length;
//            totalWriteRecords++;
//            rec.setElement(el);
        } catch (Exception ex) {
//            throw new OracleRecordQueueException("Failed to add log record to the list.", ex);
        }
    }

    /**
     * Return the next log record from the logEventList.
     *
     * @return Log record or null if the list is empty or has reached at the end of the list.
     */
//    public LogRecord getNextLogRecord() {
//        return getNextLogRecord(defaultCursor);
//    }
//
//    public LogRecord readFrom(Element e) {
//        return readFrom(e, defaultCursor);
//    }

//    public LogRecord readFrom(Element e, Object cursor) {
//        try {
//            logEventList.seek(e, cursor);
//            return getNextLogRecord(cursor);
//        } catch (IOException ex) {
//            throw new OracleLogStreamException("Seek to first log record in a TX failed.", ex);
//        }
//    }
//
//    public LogRecord getNextLogRecord(Object cursor) {
//        try {
//            Element el = logEventList.get(cursor);
//            return convertToLogRecordAndFreeMemory(el);
//        } catch (IOException ex) {
//            throw new OracleRecordQueueException("Failed to read log record from the list.", ex);
//        }
//    }

    /**
     * Return the last log record from the logEventList.
     *
     * @return Log record or null if the list is empty or has reached at the end of the list.
     */
//    public LogRecord getLastLogRecord() {
//        try {
//            Element el = logEventList.getLast();
//            return convertToLogRecordAndFreeMemory(el);
//        } catch (IOException ex) {
//            throw new OracleRecordQueueException("Failed to read log record from the list.", ex);
//        }
//    }

//    private LogRecord convertToLogRecordAndFreeMemory(Element el) {
//        LogRecord rec = null;
//        if (el != null) {
//            rec = logRecordConverter.toLogRecord(el.getData());
//            rec.setElement(el);
//            el.freeByteBuffer(); // We have consumed the cached data, so free the buffer.
//        }
//        return rec;
//    }

    /**
     * Rewind the logEventList and return the first log record.
     *
     * @return Log record or null if the list is empty
     */
    public CustomLogRecord getFirstLogRecord() {
        return getFirstLogRecord(defaultCursor);
    }

    public CustomLogRecord getFirstLogRecord(Object cursor) {
        logEventList.rewind(cursor);
//        return getNextLogRecord(cursor);
        return null;
    }

    /**
     * Found the first log entry in the logEventList.
     *
     * @return SCN of the first uncommitted log event. 0 if there is none.
     */
    public CustomLogRecord getFirstUncommitedLogRecord() {
        LogRecordList firstUncommittedEvent;
        synchronized (xidMap) {
            firstUncommittedEvent = xidMap.values().stream().findFirst().orElse(null);
            return (firstUncommittedEvent != null) ? firstUncommittedEvent.getFirstRecord() : null;
        }
    }

//    LogRecordList getTxQ(Xid xid) {
//        LogRecordList records;
//        synchronized (xidMap) {
//            records = xidMap.get(xid);
//            return records;
//        }
//    }

    /**
     * If there is a transaction list for the given commit/rollback, remove it from the xidMap, add the log to the list,
     * then return the list. If there is no transaction list, it returns null without adding it to any list.
     *
     * @param rec - commit or rollback log message. This method does not validate the type so the caller make sure to
     *     call with correct type.
     * @return the list of logEvents if transaction found, null otherwise.
     */
//    LogRecordList getTxQAndAdd(LogRecord rec) {
//        LogRecordList records;
//        synchronized (xidMap) {
//            records = xidMap.get(rec.getXid());
//        }
//        if (records != null) {
//            addToXidList(records, rec);
//        }
//        return records;
//    }

    /**
     * If there is a transaction list for the given commit/rollback, add the log to the list as well as in logEventList
     * (ByteArrayList). If there is no transaction list, it returns null without adding it to any list.
     *
     * @param - commit or rollback log message. This method does not validate the type so the caller make sure to
     *          call with correct type.
     * @return the list of logEvents if transaction found, null otherwise.
     */
//    LogRecordList getTxQAndAddToLogEventList(LogRecord rec) {
//        LogRecordList records;
//        synchronized (xidMap) {
//            records = xidMap.get(rec.getXid());
//        }
//        if (records != null) {
//            addToLogEventList(rec);
//            addToXidList(records, rec);
//        }
//        return records;
//    }

//    private void addToXidList(LogRecordList records, LogRecord rec) {
//        if (records.add(rec)) {
//            nCachedRecords.incrementAndGet();
//        }
//    }

//    LogRecordList removeFromTxQ(Xid xid) {
//        LogRecordList records;
//        synchronized (xidMap) {
//            records = xidMap.remove(xid);
//            if (records != null) {
//                nCachedXid = nCachedXid - 1;
//                nCachedRecords.addAndGet(-records.getCachedCount());
//            }
//        }
//        return records;
//    }
    public long getCachedLogSize() {
        return nCachedXid * PER_XID_MEM_USAGE + nCachedRecords.get() * PER_LOG_MEM_USAGE;
    }

    /**
     * Checkpoint the logEventList (ByteArrayList) and persist it in the permanent storage.
     */
    public void checkpoint() {
//        if (OracleService.debug) {
//            LOG.info("Skip checkpointing persistent storage in debug mode (runmock)");
//            return;
//        }
        try {
            logEventList.checkPoint();
        } catch (Exception e) {
//            throw new OracleRecordQueueException("ByteArrayOnFile checkpoint failed.", e);
        }
    }

    public void close() {
        checkpoint();
    }

    /**
     * Delete the given log record from the file backed list (ByteArrayList).
     *
     * @param l log record to delete
     */
    public void deleteFromLogEventList(CustomLogRecord l) {
        try {
//            logEventList.delete(l.element);
        } catch (Exception e) {
//            throw new OracleRecordQueueException("Unable to delete an element from ByteArrayListOnFile", e);
        }
    }

    public void cleanup() {
        logEventList.cleanup();
    }

    public long getNCachedRecords() {
        return nCachedRecords.get();
    }

    public long getNCachedXid() {
        return nCachedXid;
    }

    /**
     * @TODO need to write business logic
     * @param xid
     * @return
     */
    public LogRecordList getTxQ(Xid xid) {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param xid
     */
    public void removeFromTxQ(Xid xid) {
    }

    public CustomLogRecord getNextLogRecord() {
        return this.nextLogRecord;
    }

    public void setNextLogRecord(CustomLogRecord nextLogRecord) {
        this.nextLogRecord = nextLogRecord;
    }

    public CustomLogRecord getLastLogRecord() {
        return this.lastLogRecord;
    }

    public void setLastLogRecord(CustomLogRecord lastLogRecord) {
        this.lastLogRecord = lastLogRecord;
    }

    /**
     * @TODO: Need to write business logic
     * @param element
     */
    public void readFrom(Object element) {
    }

    /**
     * @TODO: Need to write business logic
     * @param rec
     * @return
     */
    public LogRecordList getTxQAndAddToLogEventList(CustomLogRecord rec) {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param rec
     * @return
     */
    public LogRecordList getTxQAndAdd(CustomLogRecord rec) {
        return null;
    }

    public static class OracleRecordQueueBuilder {
        private LogRecordConverter recordConverter;
        private SyncAdapter syncAdapter;
        public OracleRecordQueueBuilder withConverter(LogRecordConverter recordConverter) {
            this.recordConverter = recordConverter;
            return this;
        }

        public OracleRecordQueueBuilder withSyncAdapter(SyncAdapter syncAdapter) {
            this.syncAdapter = syncAdapter;
            return this;
        }

        public OracleRecordQueue build() {
            OracleRecordQueue queue = new OracleRecordQueue(recordConverter);
            queue.syncAdapter = syncAdapter;
            queue.start();
            return queue;
        }

        /**
         * @TODO: Need to write business logic
         * @param secretKey
         * @return
         */
        public OracleRecordQueueBuilder withSecretKey(SecretKey secretKey) {
            return null;
        }
    }
}
