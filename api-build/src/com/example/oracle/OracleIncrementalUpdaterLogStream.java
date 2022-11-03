package com.example.oracle;

import com.example.core.ColumnType;
import com.example.core.FeatureFlag;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.core.storage.PersistentStorage;
import com.example.crypto.OracleRecordQueue;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.logger.event.integration.InfoEvent;
import com.example.oracle.exceptions.InvalidScnException;
import com.example.oracle.logminer.LogMinerSession;

import javax.crypto.SecretKey;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.example.oracle.Constants.UPPER_BOUND_INTERVAL;

public class OracleIncrementalUpdaterLogStream extends OracleIncrementalUpdater {

    public static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    public static final String SQL_SEPARATOR = "~S~Q~L~";
    private static final int MAX_WAIT_FOR_EXTRACTOR_START_IN_SECONDS = 60;
    public static final long REPORT_PROGRESS_NMSG = 100_000L;

    //@TODO: Need to set size
    private static final int START_FETCH_SIZE = 0;

    //@TODO: need to set size
    private static final int DEFAULT_FETCH_SIZE = 0;

    static long reportProgressNmsg = REPORT_PROGRESS_NMSG;

    private static final int CONSECUTIVE_FAILURE_MAX_RETRIES = 3;
    private static final int TOTAL_FAILURE_MAX_RETRIES = 12;
    private boolean useLogMinerStreamPersistency;
    OracleConnectorContext context;
    OracleRecordQueue recordQueue;
    boolean preExtractionReplayCompleted = false;
    private volatile boolean extractorRunning = false;
    private Future<?> extractor;
    private long connectorCycleStartScn;
    private volatile long extractStartScn;
    private volatile Instant extractSessionStart;
    /**
     * How many log messages has been added in the byte array list at the current extractStartScn. So that we may skip
     * this many logs when we have to restart the extraction from that SCN.
     */
    private long extractStartScnCount;

    private Instant connectorCycleStatStart;
    private long connectorCycleStatTotalReceived = 0;
    private long scnExtracted = 0;
    private Instant lastCheckPointed;
    public static final Duration MIN_CHECKPOINT_DURATION = Duration.ofHours(1);
    private boolean lastIncrementalSync;
    private long totalLogProcessed = 0;

    private Duration totalProcessingTime = Duration.ZERO;
    private ExtractorFlowController extractorFlowController = new ExtractorFlowController();
    private boolean continuousMineEnabled = true;
    private int extractorConsecutiveFailureCount = 0;
    private int extractorTotalFailureCount = 0;
    Set<TableRef> logUnsupportedLogMinerOp = new HashSet<>();
    Set<TableRef> logUnsupportedSQL = new HashSet<>();
    Set<TableRef> logLobWithPkey = new HashSet<>();
    Set<TableRef> logSkipInvalidRowId = new HashSet<>();

    private TimestampCounter tsCounter = new TimestampCounter();
    private SleepControl sleepBetweenExtractorLoop = new SleepControl(10, 120_000, 5);

    CustomLogRecord debugLastGoodLogRecord = null;
    LogProcessingQ<LogRecordList> processingQ = new LogProcessingQ();
    boolean useFileAdapterForTest = FeatureFlag.check("OracleUseFileAdapterForTest"); // an internal FF for TEST.
    /**
     * nextReadScn value when a sync cycle started. Use this to check if persisted data has been successfully
     * checkpointed or not.
     */
    long startingNextReadScn;

    private boolean processingError = false;

    private TimestampConverter timestampConverter;

    private final MetaDataUtil metaDataUtil;

    private static boolean isOracleFixHistoryModeTimeStamp = FlagName.OracleFixHistoryModeTimeStamp.check();

    public OracleIncrementalUpdaterLogStream(
            OracleConnectorContext context, Map<TableRef, List<OracleColumn>> selected) {
        super(
                context.getOracleState(),
                context.getOracleApi(),
                context.getHashIdGenerator(),
                context.getOracleOutputHelper(),
                context.getOracleResyncHelper(),
                context.getOracleMetricsHelper(),
                selected);
        this.context = context;
        this.metaDataUtil = context.getMetaDataUtil();
        LogRecordConverter converter = new LogMinerRecordConverter(metaDataUtil);
        SyncAdapter syncAdapter = context.getOracleSyncAdapter();

        if (useLogMinerStreamPersistency && syncAdapter == null) {
            state.storageId = context.getStorageId();
            LOG.info("Storage ID = " + state.storageId);
            try {
                if (useFileAdapterForTest) {
                    syncAdapter = new FileSyncAdapter("/var/tmp/" + state.storageId);
                    context.oracleSyncAdapter(syncAdapter);
                } else {
                    PersistentStorage persistentStorage = context.getPersistentStorage();
                    if (persistentStorage instanceof GcsPersistentStorage
                            || persistentStorage instanceof S3PersistentStorage) {
                        syncAdapter = new CloudStorageSyncAdapter(state.storageId, persistentStorage);
                        context.oracleSyncAdapter(syncAdapter);
                    } else {
                        fallbackNonPersistency(
                                "Oracle LogMiner stream only supports GCS storage only. Fallback to non-persistent mode",
                                null);
                    }
                }
            } catch (Exception e) {
                // If we failed to access the cloud storage then fall back to non-persistenct mode.
                fallbackNonPersistency("Error accessing cloud storage. Fallback to non-persistent mode: ", e);
            }
        }
        String groupId = context.getConnectionParameters().owner;
        SecretKey secretKey = context.getEncryptionKey(state, groupId);
        recordQueue =
                new OracleRecordQueue.OracleRecordQueueBuilder()
                        .withConverter(converter)
                        .withSecretKey(secretKey)
                        .withSyncAdapter(syncAdapter)
                        .build();
        // @TODO need to uncomment below line
        // LogRecord.setOracleRecordQueue(recordQueue);
        api.setOutputHelper(outputHelper);

        determineUseOfContinousMine();
    }

    private void fallbackNonPersistency(String s, Object o) {
    }

    private void determineUseOfContinousMine() {
        try (Connection connection = api.connectToSourceDb(Container.CDB);
             LogMinerSession session = api.newLogMinerSession(connection)) {
            continuousMineEnabled = session.isContinuousMineEnabled();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "LogMiner stream, cannot determine the use of continous mine option:", e);
        }
    }

    private void fallbackNonPersistency(String msg, Exception e) {
        LOG.log(Level.WARNING, msg, e);
        useLogMinerStreamPersistency = false;
        state.nextReadScn = state.earliestUncommitedScn;
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (OracleLogStreamException e) {
            throw new RuntimeException(e);
        }
        recordQueue.close();
        recordQueue.cleanup();
    }

    @Override
    public void start() {
        preExtractionReplay();

        if (extractor == null) {
            // extractor only starts once.
            ExecutorService extractExecutor = Executors.newSingleThreadExecutor();
            extractor = extractExecutor.submit(this::extractorLoop);
            extractExecutor.shutdown();
            waitForExtractorStart();
            lastCheckPointed = Instant.now();
        }
    }

    @Override
    public void stop() throws OracleLogStreamException {
        if (!extractorRunning) {
            return;
        }
        waitForExtractorStop();
    }



    void waitForExtractorStart() {
        SleepControl sleepForExtractorStart = new SleepControl(10);
        Instant waitUntil = Instant.now().plus(MAX_WAIT_FOR_EXTRACTOR_START_IN_SECONDS, ChronoUnit.SECONDS);
        do {
            sleepForExtractorStart.sleep();
        } while (!extractorRunning && Instant.now().isBefore(waitUntil));
        if (!extractorRunning) {
            LOG.warning("The extract didn't start in " + MAX_WAIT_FOR_EXTRACTOR_START_IN_SECONDS + " seconds.");
        }
    }

    void waitForExtractorStop() throws OracleLogStreamException {
        if (!lastIncrementalSync) {
            SleepControl sleepForExtractorStop = new SleepControl(10, 10_000, 2);
            extractorRunning = false;
            while (true) {
                if (processingError) { // stop extractor and skip incremental update.
                    lastIncrementalSync = true;
                    extractor.cancel(true);
                } else {
                    lastIncrementalSync = extractor.isDone(); // check before
                    doIncrementalWork();
                    if (!useLogMinerStreamPersistency) {
                        state.nextReadScn = state.earliestUncommitedScn;
                        outputHelper.checkpoint(state);
                    }
                }
                if (lastIncrementalSync) {
                    afterFinalRunTask();
                    return;
                }
                sleepForExtractorStop.sleep();
            }
        }
    }

    private void afterFinalRunTask() throws OracleLogStreamException {
        printUncommittedTxSummary("UNCOMMITTED TX: Saving");
        if (processingError) {
            // Do not wait for the extractor to finish.
            return;
        }
        try {
            extractor.get();
        } catch (ExecutionException e) {
            if (needResync(e)) {
                throw new InvalidScnException("Fallen behind processing redo logs", e);
            }
            Throwable cause = e.getCause();
            throw new OracleLogStreamException(
                    "Extractor finished with an error: " + (cause != null ? cause.getMessage() : ""), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean needResync(Throwable e) {
        while (e != null && !(e instanceof SQLException)) {
            e = e.getCause();
        }

        if (e != null) {
            SQLException sqlException = (SQLException) e;
            return OracleErrorCode.ORA_01291.is(sqlException)
                    || OracleErrorCode.ORA_01292.is(sqlException)
                    || OracleErrorCode.ORA_01284.is(sqlException);
        }
        return false;
    }

    private void printUncommittedTxSummary(String msg) {
        long totalUncommittedRecCount = 0;
        LOG.info(msg);
        for (Map.Entry<Xid, LogRecordList> x : recordQueue.xidMap.entrySet()) {
            long nRec = x.getValue().getLogRecordList().size();
            LOG.info(String.format("UNCOMMITTED TX: XID = %s, # of log events = %d", x.getKey().toString(), nRec));
            totalUncommittedRecCount += nRec;
        }
        LOG.info(
                String.format(
                        "UNCOMMITTED TX: RECORD COUNT = %d cachedLog CACHESIZE (%d, %d, %d)",
                        totalUncommittedRecCount,
                        recordQueue.getNCachedXid(),
                        recordQueue.getNCachedRecords(),
                        recordQueue.getCachedLogSize()));
    }

    /** Run forever in a loop until the main thread stops it by signalling thru extractorRunning flag. */
    @SuppressWarnings("java:S135")
    private void extractorLoop() {
        LOG.info("LogMiner stream extractor starts.");
        Thread.currentThread().setName("log-extractor");
        long endSave = 0;
        connectorCycleStartScn = extractStartScn = state.nextReadScn.get();
        extractStartScnCount = 0;
        extractorRunning = true;
        connectorCycleStatStart = Instant.now();
        OracleLogStreamException error = null;
        SleepControl sleepForEndScn = new SleepControl(10, 10_000, 2);
        boolean retryAfterFailure;
        do {
            retryAfterFailure = false;

            long extractEndScn = continuousMineEnabled ? api.getDatabaseDao().getCurrentScn() : api.mostRecentScn();
            if (extractEndScn < extractStartScn) {
                if (extractEndScn != endSave) { // to avoid the flood of log message.
                    //LOG.info(String.format("We have reached the end SCN %s ", cNum(extractEndScn)));
                    endSave = extractEndScn;
                }
                sleepForEndScn.sleep();
                continue;
            }
            Instant extractStartTime = api.convertScnToTimestamp(extractStartScn);
            Instant extractEndTime = api.convertScnToTimestamp(extractEndScn);

            // Retrieves the current system timezone offset and initializes the timestamp converter
            //this.timestampConverter = context.getTimestampConverterBuilder().build();
            this.timestampConverter = context.getTimestampConverter();

            LOG.info(
                    String.format(
                            "Extracting LogMiner stream between %d - %d (%s - %s)",
                            extractStartScn, extractEndScn, extractStartTime.toString(), extractEndTime.toString()));
            extractSessionStart = Instant.now();
            extractorFlowController.resetTotalWait();

            //@TODO what is 2 ?
            FetchSizeTracker fetchSizeTracker =
                    new FetchSizeTracker(START_FETCH_SIZE, DEFAULT_FETCH_SIZE, Duration.ofSeconds(60), 2);

            try (Connection connection = api.connectToSourceDb(Container.CDB);
                 LogMinerSession session = getLogMinerScnWithRange(connection, extractStartScn, extractEndScn)) {
                configureSession(connection);
                session.start();

                //@TODO: Need to write logic for history mode and for getLogMinerStream
                try (PreparedStatement statement =
                             api.getLogMinerStream(
                                     connection, includedTables, outputHelper.hasHistoryMode(), fetchSizeTracker);
                     ResultSet resultSet = statement.executeQuery()) {
                    extractLogMinerStream(extractStartScn, extractStartScnCount, resultSet, fetchSizeTracker);
                    extractorConsecutiveFailureCount = 0;

                    if (extractStartScn != extractEndScn) {
                        // We return from extractLogMinerStream without error, means we have processed the
                        // whole SCN range we setup with the LogMiner session. So update the next startScn
                        // to endScn.
                        extractStartScn = extractEndScn;
                        extractStartScnCount = 0;
                    }
                }

                printRedoLogVolume(session, extractSessionStart, extractorFlowController.getTotalWait());
                updateExtractBlockVolume(session.getRedoLogFileSize());
            } catch (Exception | OracleLogStreamException e) {
                if (!extractorRunning) {
                    retryAfterFailure = true; // This lets the last extractorLoop retry up to MAX_RETRIES times.
                }
                if (needResync(e)
                        || ++extractorConsecutiveFailureCount >= CONSECUTIVE_FAILURE_MAX_RETRIES
                        || ++extractorTotalFailureCount >= TOTAL_FAILURE_MAX_RETRIES) {
                    LOG.log(
                            Level.WARNING,
                            String.format(
                                    "LogMiner stream extraction error: %s, extractorConsecutiveFailureCount:%d, extractorTotalFailureCount: %d",
                                    e.getMessage(), extractorConsecutiveFailureCount, extractorTotalFailureCount),
                            e);
                    error = (OracleLogStreamException) e;
                    break;
                }

                LOG.log(
                        Level.INFO,
                        String.format(
                                "LogMiner stream extraction error. Retrying ... extractorConsecutiveFailureCount: %d, extractorTotalFailureCount:%d",
                                extractorConsecutiveFailureCount, extractorTotalFailureCount),
                        e);
            }
            sleepBetweenExtractorLoop.sleep();
        } while (extractorRunning || retryAfterFailure);
        LOG.info("Extractor stopped");
        printExtractorStat("EndOfExtractor");
        if (error != null) try {
            throw new OracleLogStreamException("The last run of the extractor failed.", error);
        } catch (OracleLogStreamException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @TODO need to write business logic for printRedoLogVolume
     * @param session
     * @param extractSessionStart
     * @param totalWait
     */
    private void printRedoLogVolume(LogMinerSession session, Instant extractSessionStart, Duration totalWait) {
    }

    /**
     * @TODO: Need to write business logic
     * @param size
     */
    private void updateExtractBlockVolume(int size) {

    }

    private void configureSession(Connection connection) throws SQLException {
        try (PreparedStatement sessionStmt = api.getNlsDateFormat(connection)) {
            sessionStmt.execute();
        }
        try (PreparedStatement sessionStmt = api.getNlsTimestampFormat(connection)) {
            sessionStmt.execute();
        }
        try (PreparedStatement sessionStmt = api.getNlsTimestampTzFormat(connection)) {
            sessionStmt.execute();
        }
    }

    @Override
    public void resync(TableRef tableRef, String reason, Boolean logEventNow) {
        resyncHelper.addToResyncSet(tableRef, reason, logEventNow);
        state.resetTable(tableRef);
    }



    @Override
    public void doIncrementalWork() throws OracleLogStreamException {
        if (processingError) {
            throw new OracleLogStreamException("Log processing has failed already.");
        }
        try {
            processLogMinerStream();
            flushExtractVolumeLog();
        } catch (Exception e) {
            processingError = true;
            throw new OracleLogStreamException("ProcessingLogMinerStream failed.", e);
        }
        if (!lastIncrementalSync && extractor.isDone()) {
            // The extractor stopped with an error. So process it.
            waitForExtractorStop();
        }
    }

    LogMinerSession getLogMinerScnWithRange(Connection connection, long start, long end) {
        return api.newLogMinerSession(connection).scnSpan(start, end);
    }

    void preExtractionReplay() {
        if (preExtractionReplayCompleted) {
            return;
        }
        CustomLogRecord rec = recordQueue.getFirstLogRecord();
        long earliestUncommittedScn = state.earliestUncommitedScn.get();
        if (rec == null) {
            LOG.info("NO DATA FROM PREVIOUS SESSION.");
            nothingToReplay(earliestUncommittedScn);
        } else {
            startingNextReadScn = state.nextReadScn.get();
            long replayedCount = 0;
            long deletedCount = 0;
            do {
                replayedCount++;
                deletedCount += replayLog(rec, earliestUncommittedScn);
                rec = recordQueue.getNextLogRecord();
            } while (rec != null);
            CustomLogRecord uncommitted = recordQueue.getFirstUncommitedLogRecord();
            if (uncommitted == null || uncommitted.getScn() != state.earliestUncommitedScn.get()) {
                // This could happen when a table with uncommitted transaction is deleted from the sync configuration
                LOG.info("Uncommitted transaction has been deleted during the preExtractionReply.");
                state.earliestUncommitedScn =
                        (uncommitted == null) ? state.nextReadScn : Optional.of(uncommitted.getScn());
            }
            LOG.info("REPLAY LOG RECORDS: TOTAL = " + cNum(replayedCount) + " DELETED " + cNum(deletedCount));
        }
        freeAllCache();
        // Set cursor to the last element then read. So that we can continue to read from what is coming in.
        CustomLogRecord lastRecord = recordQueue.getLastLogRecord();
        if (lastRecord == null) {
            recordQueue.getFirstLogRecord();
        } else {
            recordQueue.readFrom(lastRecord.getElement());
        }
        printUncommittedTxSummary("UNCOMMITTED TX: Resumed");
        preExtractionReplayCompleted = true;
    }

    private long replayLog(CustomLogRecord rec, long earliestUncommittedScn) {
        long nDeleted;
        if (rec.getScn() < earliestUncommittedScn
                || rec.getXid().toString().endsWith("FFFFFFFF") // remove the errorneously added uncommitted tx
                || rec.getScn() > startingNextReadScn
                || rec.getScn() == startingNextReadScn && rec.getOperation() != LogMinerOperation.COMMIT_XID) {
            rec.getElement().delete();
            nDeleted = 1;
        } else {
            nDeleted = processLogInReplay(rec);
        }
        return nDeleted;
    }


    private void nothingToReplay(long earliestUncommittedScn) {
        if (!state.earliestUncommitedScn.equals(state.nextReadScn)) {
            LOG.warning(
                    String.format(
                            "It appears that the saved persistent data was lost (nextReadScn = %s) (earliestUncommitedScn = %s).",
                            state.nextReadScn, state.earliestUncommitedScn));
            state.nextReadScn = state.earliestUncommitedScn;
        }
        startingNextReadScn = earliestUncommittedScn;
    }

    private void freeAllCache() {
        for (LogRecordList l : recordQueue.xidMap.values()) {
            l.freeCache();
        }
    }

    private void extractLogMinerStream(
            long extractStartForCurrentCycle, long skipCount, ResultSet rows, FetchSizeTracker fetchSizeTracker)
            throws SQLException, OracleLogStreamException {
        rows.setFetchSize(DEFAULT_FETCH_SIZE);
        boolean warningFlag = true;
        long outOfOrder = Long.MAX_VALUE;

        while (rows.next()) {
            fetchSizeTracker.update(rows);
            extractorConsecutiveFailureCount = 0; // reset failure count
            connectorCycleStatTotalReceived++;
            //@TODO: What is SCN, seems like enum. Need to check
            scnExtracted = rows.getLong(String.valueOf(LogMinerRow.SCN));
            if (scnExtracted < extractStartScn && warningFlag) {
                LOG.info(
                        "ScnLog: extracted scn ("
                                + scnExtracted
                                + ") is smaller than extractStartScn ("
                                + this.extractStartScn
                                + ")");
                warningFlag = false;
            }
            boolean buildOnlyAndSkip = false;
            if (extractStartForCurrentCycle == scnExtracted && skipCount > 0) {
                skipCount--;
                buildOnlyAndSkip = true;
            }
            buildLogRecordAndProcess(rows, buildOnlyAndSkip);

            if (scnExtracted > extractStartScn) {
                this.extractStartScnCount = 1;
                if (outOfOrder != Long.MAX_VALUE) {
                    printOutOfOrder(outOfOrder);
                }
                outOfOrder = Long.MAX_VALUE;
                extractStartScn = scnExtracted; // must update extractStartScn after inserting into logEventList.
            } else if (scnExtracted == extractStartScn) {
                this.extractStartScnCount++;
            } else {
                // out of order
                outOfOrder = min(scnExtracted, outOfOrder);
            }
            if ((connectorCycleStatTotalReceived % reportProgressNmsg) == 0) {
                printExtractorStat("InProgress");
            }
            extractorFlowController.flowControl(recordQueue);
            fetchSizeTracker.resetLastUpdate();
        }
        if (outOfOrder != Long.MAX_VALUE) {
            printOutOfOrder(outOfOrder);
        }
        printExtractorStat("EndOfSession");
    }



    private void printOutOfOrder(long outOfOrder) {
        LOG.info("OutOfOrderScn detected: " + outOfOrder + " extractStartScn: " + extractStartScn);
    }


    private void printExtractorStat(String marker) {
        LOG.info(
                String.format(
                        "STREAM_EXTRACT: (%s) TOTAL %s records, START SCN = %s, END SCN = %s SAVED = %s (bytes) %s (records) in %s seconds %s seconds wait CACHESIZE (%d,%d,%d)",
                        marker,
                        cNum(connectorCycleStatTotalReceived),
                        cNum(connectorCycleStartScn),
                        cNum(scnExtracted),
                        cNum(recordQueue.totalWriteBytes),
                        cNum(recordQueue.totalWriteRecords),
                        durationToSeconds(Duration.between(connectorCycleStatStart, Instant.now())),
                        durationToSeconds(extractorFlowController.getAccumulatedWait()),
                        recordQueue.getNCachedXid(),
                        recordQueue.getNCachedRecords(),
                        recordQueue.getCachedLogSize()));
    }

    private void processLogMinerStream() throws OracleLogStreamException {
        final long NO_SCN_PROCESSED = -1L;
        long scnProcessed = NO_SCN_PROCESSED;
        long startScnSave;

        Instant processStatStart = Instant.now();
        Instant lastRead = processStatStart;
        boolean isExtractorStopped = isExtractorStopped();

        startScnSave = extractStartScn; // Save the next extractorScn before getting the next record.
        LogRecordList committedLogs;

        while ((committedLogs = processingQ.peak()) != null) {
            long totalLogProcessedSave = totalLogProcessed / reportProgressNmsg;
            scnProcessed = committedLogs.getLastScn();
            totalLogProcessed +=
                    processTransaction(
                            committedLogs.getXid(), LogMinerOperation.COMMIT_XID, committedLogs.getCommitTime());
            lastRead = Instant.now();
            if ((totalLogProcessed / reportProgressNmsg) != totalLogProcessedSave) {
                LOG.info(
                        String.format(
                                "STREAM_PROCESS: Total processed logs %s current SCN %s duration %s seconds",
                                cNum(totalLogProcessed),
                                cNum(scnProcessed),
                                durationToSeconds(Duration.between(processStatStart, lastRead))));
            }
            startScnSave = extractStartScn; // extractStartScn is the last scn that extractor extracted.
            isExtractorStopped = isExtractorStopped();
            processingQ.release(committedLogs);
        }
        if (lastIncrementalSync && FlagName.OracleSaveUncommittedTransactionsToJailTable.check())
            checkUncommittedTransactionsAndSaveToJailTable();

        CustomLogRecord uncommitted = (CustomLogRecord) recordQueue.getFirstUncommitedLogRecord();
        @SuppressWarnings("java:S3358")
        long uncommittedScn =
                uncommitted != null ? uncommitted.getScn() : (isExtractorStopped ? startScnSave : scnProcessed);
        if (uncommittedScn != NO_SCN_PROCESSED) {
            // No records have been captured during the current update cycle and the extractor is still running.
            if (uncommittedScn == 0L) {
                throw new OracleLogStreamException("Internal error. earliestUncommitedScn is about to be set to 0");
            }
            state.earliestUncommitedScn = Optional.of(uncommittedScn);
        }
        if (useLogMinerStreamPersistency) {
            state.nextReadScn = Optional.of(startScnSave);
        } else {
            state.nextReadScn = state.earliestUncommitedScn;
        }
        if (lastIncrementalSync || timeToCheckPoint()) {
            // Checkpoint persistent list before standard out.
            LOG.info("Checkpoint incremental sync");
            // How to determine the uncommitted SCN.
            // 1. If there is an uncommitted transaction, use the first SCN from that transaction.
            // 2. If extractor is not running anymore and we don't have any uncommitted transaction, then use the last
            // extracted SCN.
            // 3. If extractor is still running and then use the SCN from the last transaction that we processed.
            recordQueue.checkpoint();
            outputHelper.checkpoint(state);
            lastCheckPointed = Instant.now();
        }
        if (scnProcessed != NO_SCN_PROCESSED) {
            totalProcessingTime = totalProcessingTime.plus(Duration.between(processStatStart, lastRead));
            LOG.info(
                    String.format(
                            "STREAM_PROCESS: TOTAL PROCESSED = %s current SCN = %s in %s seconds",
                            cNum(totalLogProcessed), cNum(scnProcessed), durationToSeconds(totalProcessingTime)));
        }
    }

    void checkUncommittedTransactionsAndSaveToJailTable() {
        // Save transactions older than Constants.UPPER_BOUND_INTERVAL to jail table
        for (Iterator<Map.Entry<Xid, LogRecordList>> it = recordQueue.xidMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Xid, LogRecordList> ent = it.next();
            Xid xid = ent.getKey();
            LogRecordList logRecordList = ent.getValue();
            if (isTransactionOlderThanUpperBoundInterval(logRecordList)) {
                LOG.info(
                        "Transaction "
                                + xid.toString()
                                + " will be moved to Jail table since it is pending for more than "
                                + UPPER_BOUND_INTERVAL
                                + " hours.");
                it.remove();
                for (Iterator<CustomLogRecord> lit = logRecordList.iterator(); lit.hasNext(); ) {
                    CustomLogRecord logRecord = lit.next();
                    writeToJailTable(logRecord);
                    recordQueue.deleteFromLogEventList(logRecord);
                }
            }
        }
    }


    void writeToJailTable(CustomLogRecord logRecord) {

        Map<String, Object> row = OracleJailTableHelper.buildRow(logRecord);
        TableRef tableRef = OracleJailTableHelper.getDestJailTableRef(logRecord.getSchema());
        outputHelper.saveLogminerJailTableDef(tableRef);
        api.logUncommittedTransactionWarning(logRecord.getXid().toString(), logRecord.getTable());
        forEachChange(
                tableRef,
                logRecord.getRowId(),
                row,
                ChangeType.changeType(LogMinerOperation.INSERT_OP),
                Instant.EPOCH,
                Optional.empty(),
                includedTables);
    }


    boolean isTransactionOlderThanUpperBoundInterval(LogRecordList logRecordList) {
        CustomLogRecord logRecord = logRecordList.getFirstRecord();
        while (logRecord != null) {
            if (!isCreatedTimeOlderThanUpperBoundInterval(logRecord)) {
                return false;
            }
            logRecord = logRecordList.getNextRecord();
        }
        return true;
    }


    boolean isCreatedTimeOlderThanUpperBoundInterval(CustomLogRecord logRecord) {
        // This is to cover records from persistent storage
        if (null == logRecord.getTimestamp()) {
            //@TODO: Need to check below line. What is oracleScn? Currently removing oracleScn
            //  Instant timestampFromScn = api.oracleScn.convertScnToTimestamp(logRecord.getScn());
            Instant timestampFromScn = api.convertScnToTimestamp(logRecord.getScn());
            logRecord.setTimestamp(timestampFromScn);
        }
        return extractSessionStart.isAfter(logRecord.getTimestamp().plus(UPPER_BOUND_INTERVAL));
    }

    private boolean isExtractorStopped() {
        return extractor.isDone();
    }

    private boolean timeToCheckPoint() {
        Duration sinceLastCheckPoint = Duration.between(lastCheckPointed, Instant.now());
        return sinceLastCheckPoint.compareTo(MIN_CHECKPOINT_DURATION) > 0;
    }


    private TableRef getTableFromRow(ResultSet rows) throws SQLException {
        String schema = rows.getString(String.valueOf(LogMinerRow.SGE_OWNER));
        String name = rows.getString(String.valueOf(LogMinerRow.TABLE_NAME));

        return (schema != null && name != null) ? metaDataUtil.getTableRef(schema, name) : null;
    }


    void buildLogRecordAndProcess(ResultSet rows, boolean buildOnlyAndSkip) throws SQLException, OracleLogStreamException {
        LogMinerOperation opCode = LogMinerOperation.valueOf(rows.getInt(LogMinerRow.OPERATION_CODE));
        byte[] xidRaw = rows.getBytes(LogMinerRow.XID);
        Xid xid = new CustomXid(xidRaw);

        CustomLogRecord rec = createLogRecord(xid, opCode, rows);
        if (null != rec && !buildOnlyAndSkip) {
            processLogInExtractor(rec);
        }
    }


    int processLogInReplay(CustomLogRecord rec) {
        int ret = 0;
        LogMinerOperation op = rec.getOperation();
        switch (op) {
            case INSERT_OP:
            case DELETE_OP:
            case UPDATE_OP:
            case DDL_OP:
                if (includedTables.containsKey(rec.getTable())) {
                    recordQueue.addToTxQueue(rec);
                    rec.freeCache();
                } else {
                    //rec.element.delete();
                    rec.getElement().delete();
                    ret++;
                }
                break;
            case COMMIT_XID:
            case ROLLBACK_XID:
                return replayTransaction(rec);
            default:
                LOG.info("Skipping unknown log miner operation code " + rec.getOperation().toString());
        }
        return ret;
    }

    void processRollback(CustomLogRecord rec) {
        LogRecordList txQ = recordQueue.getTxQ(rec.getXid());
        if (txQ == null) {
            return; // irrelevant commit or rollback event.
        }
        if (rec.getOperation() == LogMinerOperation.DELETE_OP) {
            for (Iterator<CustomLogRecord> it = txQ.iterator(); it.hasNext(); ) {
                CustomLogRecord logRecord = it.next();
                if (logRecord.getRowId().equalsIgnoreCase(rec.getRowId())
                        && logRecord.getOperation() == LogMinerOperation.INSERT_OP) {
                    recordQueue.deleteFromLogEventList(logRecord);
                    it.remove();
                }
            }
        }
    }

    int processLogInExtractor(CustomLogRecord rec) throws OracleLogStreamException {
        int ret = 0;
        LogMinerOperation op = rec.getOperation();
        switch (op) {
            case INSERT_OP:
            case DELETE_OP:
            case UPDATE_OP:
            case DDL_OP:
                if (includedTables.containsKey(rec.getTable()) && !state.isBeforeStartingTableImport(rec.getTable())) {
                    if (FlagName.OracleProcessRollbackInserts.check()
                            && rec.getRollbackInfo() == RollbackInfo.ROLLBACK) {
                        processRollback(rec);
                    } else {
                        recordQueue.addToLogEventList(rec);
                        recordQueue.addToTxQueue(rec);
                        rec.freeCache();
                    }
                }
                break;
            case COMMIT_XID:
                LogRecordList xidList = recordQueue.getTxQAndAddToLogEventList(rec);
                if (null != xidList) {
                    xidList.closeForWrite(rec.getTimestamp());
                    processingQ.add(xidList);
                }
                break;
            case ROLLBACK_XID:
                processTransaction(rec.getXid(), rec.getOperation(), rec.getTimestamp());
                break;
            default:
                LOG.info("Skipping unknown log miner operation code " + rec.getOperation().toString());
        }
        return ret;
    }


    @SuppressWarnings("java:S3776")
    long processTransaction(Xid xid, LogMinerOperation op, Instant commitTime) throws OracleLogStreamException {
        int processedLogCount = 0;
        LogRecordList txQ = recordQueue.getTxQ(xid);
        if (txQ == null) {
            return 0; // irrelevant commit or rollback event.
        }
        recordQueue.removeFromTxQ(xid);
        // Commit old uncommitted transaction from the current sync. We must save all log Event until next sync
        // when we know if they are successfully checkpointed.
        boolean isCommit = op == LogMinerOperation.COMMIT_XID;
        boolean commitOldUncommttedRecord = false; // any committed transaction remain in the persistent list.
        Map<TableRef, List<ParserResult>> savedForRowIdMap = new HashMap<>();
        boolean sizeWarningPrinted = false;
        for (Iterator<CustomLogRecord> it = txQ.iterator(); it.hasNext(); ) {
            CustomLogRecord logRecord = it.next();
            LogMinerOperation operation = logRecord.getOperation();
            boolean deleteRecord = true;

            if (isCommit) {
                deleteRecord = logRecord.getScn() >= startingNextReadScn;
                if (operation != LogMinerOperation.COMMIT_XID) {
                    if (!isOracleFixHistoryModeTimeStamp) logRecord.setTimestamp(commitTime);
                    /*
                     * Inserting into a table with LOB columns creates two log records.
                     * 1. Insert with non-lob-column values only and set LOB column wit EMPTY_CLOB() or EMPTY_BLOB()
                     *    The RowID of this log event is All As.
                     * 2. update to only lob columns. This records has valid RowId.
                     *
                     * The sync strategy is that we detect the insert event for such table. Save the insert event and
                     * wait for the following update event to grab the correct rowId. Take only the RowId and
                     * throw away this update log. Use the new & valid rowId process the saved insert log record.
                     */
                    List<ParserResult> savedForRowId =
                            savedForRowIdMap.computeIfAbsent(logRecord.getTable(), k -> new ArrayList<>());
                    if (!savedForRowId.isEmpty() && logRecord.getOperation() == LogMinerOperation.UPDATE_OP) {
                        ParserResult found = null;
                        ParserResult newRow = parseLogRecord(logRecord);
                        for (ParserResult savedRow : savedForRowId) {
                            if (savedRow.compareNewValues(newRow)) {
                                found = savedRow;
                                break;
                            }
                        }
                        if (null != found) {
                            CustomLogRecord rec = found.getRecord();
                            rec.setRowId(logRecord.getRowId());
                            processCommittedRecord(rec);
                            savedForRowId.remove(found);
                        } else {
                            processCommittedRecord(logRecord);
                        }
                        if (!sizeWarningPrinted && savedForRowId.size() > 10) {
                            LOG.warning("TableLog: SavedForRowId grew too large " + savedForRowId.size());
                            sizeWarningPrinted = true;
                        }
                    } else if (isNeededToSaveForRowId(logRecord)) {
                        ParserResult parserResult = parseLogRecord(logRecord);
                        savedForRowId.add(parserResult);
                    } else {
                        processCommittedRecord(logRecord);
                    }
                } else if (commitOldUncommttedRecord) {
                    // Do not delete commit record if we still keep the old uncommitted record that is committed by this
                    deleteRecord = false;
                }
            }
            if (deleteRecord) {
                recordQueue.deleteFromLogEventList(logRecord);
            } else {
                commitOldUncommttedRecord = true;
            }
            processedLogCount++;
            it.remove();
        }

        if (!savedForRowIdMap.isEmpty()) {
            for (List<ParserResult> savedForRowId : savedForRowIdMap.values()) {
                if (!savedForRowId.isEmpty()) {
                    CustomLogRecord firstRecord = savedForRowId.get(0).getRecord();
                    LOG.warning(
                            "TableLog: Missing update for insert count = "
                                    + savedForRowId.size()
                                    + " TABLE = "
                                    + firstRecord.getTable()
                                    + " first data = "
                                    + firstRecord.getRawData());
                }
            }
        }
        return processedLogCount;
    }

    private boolean isNeededToSaveForRowId(CustomLogRecord logRecord) {
        final String rowIdWithAllAs = "AAAAAAAAAAAAAAAAAA";
        if (logRecord.getOperation() != LogMinerOperation.INSERT_OP
                || !rowIdWithAllAs.equals(logRecord.getRowId())
                || !hasEmptyLob((String) logRecord.getRawData())) {
            return false;
        }
        TableRef table = logRecord.getTable();
        List<OracleColumn> columns = includedTables.get(table);
        List<OracleColumn> pkeyList = getPkeyList(table, columns);
        if (!logLobWithPkey.contains(table)) {
            logLobWithPkey.add(table);
            LOG.info(
                    String.format(
                            "TableLog: insert event with PKEY: table = %s pkeyless = %s", table, pkeyList.isEmpty()));
        }
        // A table with a primary key or a table with row movement enabled does not need a rowid.
        return pkeyList.isEmpty() && !shouldUseHashIdPkey(table);
    }


    private boolean hasEmptyLob(String rawData) {
        return (rawData != null && (rawData.contains("EMPTY_BLOB()") || rawData.contains("EMPTY_CLOB()")));
    }


    int replayTransaction(CustomLogRecord rec) {
        int processedLogCount = 0;
        LogRecordList txQ = recordQueue.getTxQAndAdd(rec);
        recordQueue.removeFromTxQ(rec.getXid());
        if (null == txQ) {
            recordQueue.deleteFromLogEventList(rec);
            return 0;
        }
        for (CustomLogRecord logRecord : txQ.getLogRecordList()) {
            recordQueue.deleteFromLogEventList(logRecord);
            processedLogCount++;
        }
        return processedLogCount;
    }


    private StringBuilder getSql(ResultSet rows, int columnOffset) throws SQLException {
        String sql = rows.getString(columnOffset);
        if (sql == null) {
            return new StringBuilder();
        } else {
            return new StringBuilder(sql);
        }
    }

    private Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? applyHistoryModeFix(timestampConverter.convert(timestamp)) : Instant.EPOCH;
    }


    private Instant applyHistoryModeFix(Instant tsInstant) {
        return (isOracleFixHistoryModeTimeStamp ? tsCounter.getValue(tsInstant) : tsInstant);
    }

    private CustomLogRecord createLogRecord(Xid xid, LogMinerOperation op, ResultSet rows) throws SQLException, OracleLogStreamException {
        CustomLogRecord rec = new CustomLogRecord(xid);
        String rowId = rows.getString(LogMinerRow.ROW_ID);
        TableRef table = getTableFromRow(rows);
        rec.setScn(rows.getLong(String.valueOf(LogMinerRow.SCN)));
        rec.setTimestamp(toInstant(rows.getTimestamp(LogMinerRow.TIMESTAMP)));
        rec.setOperation(op);
        rec.setTable(table);
        rec.setRowId(rowId);

        RollbackInfo rollbackInfo = RollbackInfo.valueOf(rows.getInt(LogMinerRow.ROLLBACK));
        rec.setRollbackInfo(rollbackInfo);

        StringBuilder redoSql;
        StringBuilder undoSql;
        if (op == LogMinerOperation.COMMIT_XID) {
            redoSql = new StringBuilder("commit;");
            undoSql = new StringBuilder();
        } else if (op == LogMinerOperation.ROLLBACK_XID) {
            redoSql = new StringBuilder("rollback;");
            undoSql = new StringBuilder();
        } else {
            redoSql = getSql(rows, LogMinerRow.SQL_REDO);
            undoSql = getSql(rows, LogMinerRow.SQL_UNDO);
        }

        int howManyCSF = 0;
        int csf = rows.getInt(LogMinerRow.CSF);
        while (csf == 1) {
            howManyCSF++;
            if (!rows.next()) {
                throw new OracleLogStreamException(
                        "Failed to parse logMiner No more row when CSF == 1 : " + redoSql + " : " + undoSql);
            }
            redoSql.append(getSql(rows, LogMinerRow.SQL_REDO));
            undoSql.append(getSql(rows, LogMinerRow.SQL_UNDO));
            csf = rows.getInt(LogMinerRow.CSF);
        }
        debugSanityCheckSql(rec.getScn(), redoSql.toString(), howManyCSF, null);
        debugSanityCheckSql(rec.getScn(), undoSql.toString(), howManyCSF, null);
        if (undoSql.length() > 0) {
            redoSql.append(SQL_SEPARATOR);
            redoSql.append(undoSql);
        }
        String data = redoSql.toString();
        rec.setRawData(data);
        if (rec.getOperation() == LogMinerOperation.UNSUPPORTED && !logUnsupportedLogMinerOp.contains(table)) {
            LOG.info(
                    String.format(
                            "TableLog: UNSUPPORTED_OPERATION: table = %s SCN = %d", tableName(table), rec.getScn()));
            logUnsupportedLogMinerOp.add(table);
            // Whether or not we will need to resync this table? To be determined.
            rec = null;
        } else if ((data.startsWith("U") || data.contains(SQL_SEPARATOR + "U"))) {
            if (!logUnsupportedSQL.contains(table)) {
                LOG.info(
                        String.format(
                                "TableLog: UNSUPPORTED_SQL: table = %s SCN = %d XID = %s SQL = %s ",
                                tableName(table), rec.getScn(), xid.toString(), data));
                logUnsupportedSQL.add(table);
                // comment out temorarily. resync(table, "Incorrect supplemental logging setting", true)
            }
            rec = null;
        }
        return rec;
    }

    private String tableName(TableRef table) {
        return (table == null) ? "<None>" : table.toString();
    }

    private void debugSanityCheckSql(long scn, String sql, int csfCount, String origStr) {
        if (sql != null && sql.length() > 0) {
            switch (sql.charAt(0)) {
                case 'r':
                case 'c':
                case 'i':
                case 'u':
                case 'd':
                    return;
                default:
                    if (sql.contains("Unsupported") || sql.contains("alter table ")) {
                        return;
                    }
                    LOG.warning(
                            String.format("Invalid SQL from LogMiner: SCN = %d CSF = %d SQL = %s", scn, csfCount, sql));
                    if (origStr != null) LOG.warning("Orig Str: " + origStr);
                    break;
            }
        }
    }

    private void processCommittedRecord(CustomLogRecord rec) throws OracleLogStreamException {
        TableRef table = rec.getTable();
        if (!includedTables.containsKey(table) || resyncHelper.tableNeedsResync(table)) {
            return;
        }
        if (rec.getOperation() == LogMinerOperation.DDL_OP) {
            processDdlEvent(rec);
            return;
        }
        ParserResult parserResult = parseLogRecord(rec);
        List<OracleColumn> pkeyList = parserResult.getPkeyList();
        if (!shouldUseHashIdPkey(table) && pkeyList.isEmpty() && RowUtils.isInvalidRowId(rec.getRowId())) {
            // For a table with a primary key, we may accept the row with all As'
            // See isNeededToSaveForRowId().
            if (!logSkipInvalidRowId.contains(table)) {
                logSkipInvalidRowId.add(table);
                LOG.warning("TableLog: Skip log event with invalid rowid. table = " + table);
            }
            return;
        }
        Map<String, Object> newValues = parserResult.getNewValues();
        Map<String, Object> oldValues = parserResult.getOldValues();
        List<OracleColumn> columns = includedTables.get(table);
        Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys = Optional.empty();
        if (rec.getOperation() == LogMinerOperation.UPDATE_OP && !pkeyList.isEmpty()) {
            maybeRowPrimaryKeys = Optional.of(buildPkeys(pkeyList, newValues, oldValues));
        }

        //@TODO: Need to assign values in pKeylessTables
        HashMap pKeylessTables = new HashMap();

        boolean isUpdateForPKlessTableUsingHashId =
                rec.getOperation() == LogMinerOperation.UPDATE_OP
                        && pKeylessTables.containsKey(table)
                        && FlagName.OracleFixPKlessTableWithRowMovement.check()
                        && (shouldUseHashIdPkey(table));
        //@TODO: Need to verify how to get value of oracleLogMinerValueConverter variable
        OracleLogMinerValueConverter oracleLogMinerValueConverter = new OracleLogMinerValueConverter();
        LogMinerStreamDMLParser parser =
                new LogMinerStreamDMLParser(table, columns, outputHelper, oracleLogMinerValueConverter);
        parser.convertStr2NativeType(newValues);
        if (rec.getOperation() == LogMinerOperation.DELETE_OP || isUpdateForPKlessTableUsingHashId) {
            parser.convertStr2NativeType(oldValues);
        }
        // The following 3 lines are debug code. To be deleted.
        debugLastGoodLogRecord = new CustomLogRecord(rec.getXid());
        debugLastGoodLogRecord.cached = rec.cached;
        debugLastGoodLogRecord.setScn(rec.getScn());
        if (isUpdateForPKlessTableUsingHashId) {
            forEachChange(
                    table,
                    rec.getRowId(),
                    oldValues,
                    ChangeType.changeType(LogMinerOperation.DELETE_OP),
                    rec.getTimestamp(),
                    maybeRowPrimaryKeys,
                    includedTables);
            forEachChange(
                    table,
                    rec.getRowId(),
                    newValues,
                    ChangeType.changeType(LogMinerOperation.INSERT_OP),
                    rec.getTimestamp(),
                    maybeRowPrimaryKeys,
                    includedTables);
        } else {
            ChangeType changeType = ChangeType.changeType(rec.getOperation());
            forEachChange(
                    table,
                    rec.getRowId(),
                    changeType == ChangeType.DELETE ? oldValues : newValues,
                    changeType,
                    rec.getTimestamp(),
                    maybeRowPrimaryKeys,
                    includedTables);
        }
    }


    private ParserResult parseLogRecord(CustomLogRecord rec) throws OracleLogStreamException {
        TableRef table = rec.getTable();
        List<OracleColumn> columns = includedTables.get(table);
        List<OracleColumn> pkeyList = getPkeyList(table, columns);
        String redoSql;
        String undoSql;
        String data = (String) rec.getRawData();
        String[] sqls = data.split(SQL_SEPARATOR);
        if (sqls.length != 2) {
            redoSql = sqls[0];
            undoSql = null;
        } else {
            redoSql = sqls[0];
            undoSql = sqls[1];
        }
        debugSanityCheckSql(rec.getScn(), redoSql, 0, data);
        debugSanityCheckSql(rec.getScn(), undoSql, 0, data);
        Map<String, Object> newValues;
        Map<String, Object> oldValues;
        try {

            if (rec.getOperation() == LogMinerOperation.DELETE_OP) {
                newValues = new HashMap<>();
                if (undoSql != null && undoSql.length() > 0 && undoSql.charAt(0) == 'i') {
                    oldValues = getValuesFromSql(undoSql, rec, table, columns);
                } else {
                    oldValues = getValuesFromSql(redoSql, rec, table, columns);
                }
            } else {
                newValues = getValuesFromSql(redoSql, rec, table, columns);
                oldValues = getValuesFromSql(undoSql, rec, table, columns);
            }

        } catch (Exception e) {
            String errorMsg = "Error in parsing SQL table = " + table + " SCN = " + rec.getScn();
            LOG.log(Level.SEVERE, errorMsg, e);
            if (debugLastGoodLogRecord != null && debugLastGoodLogRecord.cached != null) {
                LOG.info(
                        "SQL from the previous good Log record: XID: "
                                + debugLastGoodLogRecord.getXid().toString()
                                + " SCN: "
                                + debugLastGoodLogRecord.getScn()
                                + " SQL: "
                                + debugLastGoodLogRecord.getRawData());
            }
            // do not propagate exception from the parser as it includes the user data.
            throw new OracleLogStreamException(errorMsg);
        }
        return new ParserResult(rec, newValues, oldValues, pkeyList);
    }

    private Map<String, Object> getValuesFromSql(
            String sql, CustomLogRecord rec, TableRef table, List<OracleColumn> columns) {
        LogMinerStreamDMLParser parser = new LogMinerStreamDMLParser(table, columns, outputHelper);
        return (sql != null && sql.length() > 0) ? parser.getRow(sql, rec.getXid()) : new HashMap<>();
    }

    private void updateSchemaForDDL(AlterTableColumnChange columnChange, CustomLogRecord rec) {
        SyncMode syncMode = outputHelper.syncModes().get(rec.getTable());
        Instant opTime = rec.getTimestamp();

        // It is required to be set in UpdateSchemaOperation which should be destination side format
        TableRef tableRef = outputHelper.tableRefWithSchemaPrefix(rec.getTable());

        if (columnChange.op == AlterOperation.ADD) {
            Optional<DataType> warehouseType = oracleType(columnChange.columnType).getWarehouseType();
            if (warehouseType.isPresent()) {
                if (isValidDefaultValue(warehouseType.get(), columnChange.defaultValue)) {
                    ColumnType columnType =
                            outputHelper.existingTableDefinition(rec.getTable()).types.get(columnChange.columnName);
                    if (columnType == null) {
                        columnType = ColumnType.fromType(warehouseType.get(), false);
                    }

                    UpdateSchemaOperation updateSchemaOperation =
                            columnChange.toUpdateSchemaOperation(tableRef, syncMode, columnType, opTime);
                    outputHelper.updateSchema(rec.getTable(), updateSchemaOperation);
                } else {
                    throw new InvalidDefaultValue(warehouseType.get(), columnChange.defaultValue);
                }
            }
        } else if (columnChange.op == AlterOperation.DROP) {
            UpdateSchemaOperation updateSchemaOperation =
                    columnChange.toUpdateSchemaOperation(tableRef, syncMode, null, opTime);
            outputHelper.updateSchema(rec.getTable(), updateSchemaOperation);
        } else {
            throw new UnsupportedOperationException(columnChange.op + " not support for DDL handling");
        }
    }


    private static boolean isValidDefaultValue(DataType dataType, String defaultValue) {
        if (dataType == DataType.LocalDate && "CURRENT_DATE".equalsIgnoreCase(defaultValue)) {
            return false;
        }

        return true;
    }

    public static class InvalidDefaultValue extends RuntimeException {
        final DataType dataType;
        final String defaultValue;

        public InvalidDefaultValue(DataType dataType, String defaultValue) {
            this.dataType = dataType;
            this.defaultValue = defaultValue;
        }
    }

    private static Integer nThOrNull(int n, List<String> list) {
        if (list == null || n >= list.size()) return null;

        return Integer.parseInt(list.get(n));
    }

    public static OracleType oracleType(ColDataType columnType) {
        List<String> argumentsStringList = columnType.getArgumentsStringList();
        if (columnType.getDataType().toLowerCase().contains("number")) {
            return OracleType.create(
                    columnType.getDataType(),
                    null,
                    nThOrNull(0, argumentsStringList),
                    nThOrNull(1, argumentsStringList));
        } else {
            return OracleType.create(columnType.getDataType());
        }
    }

    public void processDdlEvent(CustomLogRecord rec) {
        String ddlEvent = rec.getRawData().toString();
        OracleDdlParser parser = new OracleDdlParser(ddlEvent);
        boolean shouldResyncDueToDdl = parser.shouldResync();

        if (shouldResyncDueToDdl) {
            boolean needsResync = true;
            if (includedTables.get(rec.getTable()).stream().noneMatch(a -> a.primaryKey)) {
                LOG.info("PK-less table needs to be re-synced");
            } else if (parser.alterShouldResync()) {
                try {
                    List<AlterTableColumnChange> columnChanges = AlterTableColumnChange.parse(ddlEvent);
                    if (columnChanges.isEmpty()) {
                        LOG.warning(DdlEvent.failure(ddlEvent, "Not able to recognize query"));
                    } else {
                        columnChanges.forEach(columnChange -> updateSchemaForDDL(columnChange, rec));
                        needsResync = false;
                    }
                } catch (InvalidDefaultValue e) {
                    LOG.warning(
                            String.format(
                                    "Found invalid default value: %s for data type %s", e.defaultValue, e.dataType));
                }
            }

            if (needsResync) {
                String ddlMessage = "DDL detected: " + ddlEvent.trim();
                LOG.info("DDL detected for table: " + rec.getTable() + "  SCN: " + rec.getScn() + " " + ddlMessage);
                resync(rec.getTable(), ddlMessage, true);
            }
        } else if (parser.isTruncate()) {
            // trigger soft delete
            outputHelper.preImportDelete(rec.getTable(), Instant.now());
            LOG.customerWarning(InfoEvent.of("TRUNCATE DDL", "Encountered TRUNCATE DDL for " + rec.getTable()));
        }
    }

    // For test only.
    public void setSleepBetweenExtractorLoop(SleepControl sleepBetweenExtractorLoop) {
        this.sleepBetweenExtractorLoop = sleepBetweenExtractorLoop;
    }

    static class ExtractorFlowController {
        //        public static final long MAX_MEMORY_USAGE_OF_LOGS_BEFORE_FLOW_CONTROL = 256L * 1024 * 1024; // 256 MB
        public static final long MAX_MEMORY_USAGE_OF_LOGS_BEFORE_FLOW_CONTROL = 256L * 1024 * 1024; // 256 MB
        private int stateChangeCount = 0;
        private boolean slowdownRequested = false;
        Duration totalWait = Duration.ZERO;
        Duration accumulatedWait = Duration.ZERO;
        SleepControl sleepForFlowControl;

        ExtractorFlowController() {
            this(new SleepControl(1_000));
        }

        ExtractorFlowController(SleepControl sleepControl) {
            sleepForFlowControl = sleepControl;
        }

        void flowControl(OracleRecordQueue recordQueue) {
            boolean newStatus = recordQueue.getCachedLogSize() > MAX_MEMORY_USAGE_OF_LOGS_BEFORE_FLOW_CONTROL;
            if (newStatus != slowdownRequested) {
                if ((stateChangeCount++ % 10) <= 1) {
                    printLog(
                            String.format(
                                    "STREAM_FLOWCONTROL: %s (stateChange count = %d, totalWait = %s secs CACHESIZE (%d, %d, %d)",
                                    (newStatus ? " slowdown" : " resume full speed"),
                                    stateChangeCount,
                                    durationToSeconds(totalWait),
                                    recordQueue.getNCachedXid(),
                                    recordQueue.getNCachedRecords(),
                                    recordQueue.getCachedLogSize()));
                }
                slowdownRequested = newStatus;
            }
            if (slowdownRequested) {
                Instant waitStart = Instant.now();
                sleepForFlowControl.sleep();
                Duration delta = Duration.between(waitStart, Instant.now());
                totalWait = totalWait.plus(delta);
                accumulatedWait = accumulatedWait.plus(delta);
            }
        }

        void printLog(String msg) {
            LOG.info(msg);
        }

        Duration getTotalWait() {
            return totalWait;
        }

        Duration getAccumulatedWait() {
            return accumulatedWait;
        }

        void resetTotalWait() {
            totalWait = Duration.ZERO;
        }
    }

    //#################################################################
    /**@TODO: Need to add business logic
     *
     */
    private void flushExtractVolumeLog() {
    }

    /**
     * @TODO: Need to write business logic
     * @param logRecord
     */
    private void writeToJailTable(LogRecord logRecord) {
    }

    /**
     * @TODO: Need to write business logic
     * @param scnExtracted
     * @param outOfOrder
     * @return
     */
    private long min(long scnExtracted, long outOfOrder) {
        return -1;
    }

    /**
     * @TODO: Need to write business logic
     * @param between
     * @return
     */
    private static Object durationToSeconds(Duration between) {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param totalLogProcessed
     * @return
     */
    private Object cNum(long totalLogProcessed) {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param table
     * @param columns
     * @return
     */
    private List<OracleColumn> getPkeyList(TableRef table, List<OracleColumn> columns) {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param table
     * @return
     */
    private boolean shouldUseHashIdPkey(TableRef table) {
        return Boolean.TRUE;
    }

    /**
     * @TODO: Need to write business logic
     * @param scn
     * @param toString
     * @param howManyCSF
     * @param o
     */
    private void debugSanityCheckSql(long scn, String toString, int howManyCSF, Object o) {
    }

    /**
     * @TODO: Need to write business logic
     * @param pkeyList
     * @param newValues
     * @param oldValues
     * @return
     */
    private Transactions.RowPrimaryKeys buildPkeys(List<OracleColumn> pkeyList, Map<String, Object> newValues, Map<String, Object> oldValues) {
        return null;
    }
}
