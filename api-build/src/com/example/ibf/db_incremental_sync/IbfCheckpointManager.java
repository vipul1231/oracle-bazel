package com.example.ibf.db_incremental_sync;

import com.example.ibf.*;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.metrics.IbfTimerSampler;
import com.example.logger.ExampleLogger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.Callable;

public class IbfCheckpointManager<Adapter extends IbfTableEncoder> {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final IbfRetrier RETRIER = new IbfRetrier(ImmutableList.of(IOException.class));
    private static final int MAX_RETRIES = 4;
    private static final int DEFAULT_SMALL_CELL_COUNT = 5_000; // results in a ~8MB ResizableIBF
    @VisibleForTesting static final int MINIMUM_SMALL_CELL_COUNT = 3_200;

//    @VisibleForTesting final int maxSmallCellCount;
    private static final double ALPHA = 1.5; // space overhead (IBF cells/number of differences)
    private static final double ESTIMATE_OVERHEAD = 1.5; // IBF cells estimation overhead

    private final Adapter adapter;
    private final String objectID;
    private final IbfPersistentStorage storage;
    private final ResizableInvertibleBloomFilter.Sizes startingSize;

    public static boolean debug = false;
    @VisibleForTesting public boolean disableMinimumSmallCellCount = false;

    private final IbfSyncData.Serializer syncDataSerializer = new IbfSyncData.Serializer();
    private final DiffManager diffManager = new DiffManager();

    @VisibleForTesting ResizableInvertibleBloomFilter.Sizes currentSize;
    @VisibleForTesting int lastRecordCount = 0;
    @VisibleForTesting boolean updatePerformedReplacement = false;

    public IbfCheckpointManager(Adapter adapter, IbfPersistentStorage storage, String objectID) {
        this(adapter, storage, objectID, ResizableInvertibleBloomFilter.Sizes.XLARGE);
    }

    /** Temporary workaround for Elasticsearch until we refactor in T-187953 */
    public IbfCheckpointManager(
            Adapter adapter,
            IbfPersistentStorage storage,
            String objectID,
            ResizableInvertibleBloomFilter.Sizes startingSize) {
        this.adapter = adapter;
        this.storage = storage;
        this.objectID = objectID;
        this.startingSize = startingSize;
//        this.maxSmallCellCount =
//                FlagName.IbfMaxSmallCellCountBasedOnHeap.check() ? computeMaxSmallCellCountWithHeapSize() : 50_000;
    }

    /**
     * Fetches a new starting-sized ResizableIBF from the database and saves it to storage, overwriting any existing,
     * persisted IBF in storage.
     *
     * <p>This method is used for the initial checkpoint capture and whenever a resync is necessary.
     *
     * @throws SQLException
     */
    public void reset() throws SQLException {
        persistIbfSyncData(new IbfSyncData(fetchStartingSizedIBF(), lastRecordCount));
    }

    /**
     * Retrieve the previously persisted IBF from storage and a new IBF from the database and then decode the difference
     * to retrieve upserts and deletes that occurred in the database table since the last checkpoint.
     *
     * <p>The diff process is:
     *
     * <ol>
     *   <li>Only if we are performing a replacement, capture the replacement IBF that will overwrite the persistedIBF
     *       at the end of this sync
     *   <li>Download the persisted IBF from cloud storage and save to disk (via`SecureTempFileStoredIBF`)
     *   <li>Fetch the latest IBF of the determined size from the source database and save to disk
     *   <li>Resize the persisted IBF
     *   <li>Compare the resized persisted IBF to the latest IBF
     *   <li>If the comparison fails, go back to step 3 and try again with the starting size
     *   <li>If the comparison succeeds, return the result
     * </ol>
     *
     * @return the result of the IBF decode operation
     * @throws IOException
     * @throws SQLException
     */
    public IbfSyncResult diff() throws IOException, SQLException {
        if (adapter.ifReplacementRequired()) diffManager.setReplacementIBF(fetchStartingSizedIBFWithColumnDefaults());

        ResizableInvertibleBloomFilter persistedIBF = downloadPersistedIBF();
        int smallCellCount = persistedIBF.smallCellCount;
        diffManager.setPreviousIBF(persistedIBF);

        currentSize = determineIBFSize(smallCellCount);

        for (int retry = 0; retry < 2; retry++) {
            diffManager.setLatestIBF(fetchSizedIBF(smallCellCount));

            runWithDurationReport(
                    IbfTimerSampler.IBF_IBF_DECODE,
                    () -> {
                        diffManager.resizePreviousIBF(currentSize);
                        diffManager.compare();
                    });

            if (diffManager.compareSucceeded()) {
//                IbfMetricsLogging.reportIBFSizePerTable(
//                        diffManager.latestIBFDataSize(),
//                        TagName.TABLE.toString(),
//                        adapter.tableRef().toString(),
//                        TagName.IBF_TYPE.toString(),
//                        IbfSubPhaseTags.LATEST_IBF.value(),
//                        TagName.RESIZABLE_IBF_SIZE.toString(),
//                        currentSize.name());
//
//                IbfMetricsLogging.reportDataSize(
//                        MetricDefinition.IBF_IBF_RESULT_SIZE,
//                        diffManager.resultDataSize(),
//                        TagName.TABLE.toString(),
//                        adapter.tableRef().toString());
                break;
            } else if (currentSize != startingSize) currentSize = startingSize;
            else break;
        }

        // Log metadata for debugging when the compare did not succeed
        if (!diffManager.compareSucceeded()) {
            DecodedIBFDebugger decodedIBFDebugger = new DecodedIBFDebugger(diffManager.diffIBF);
//            LOG.info(
//                    String.format(
//                            "Failed to decode result because %s: %s",
//                            decodedIBFDebugger.determineDecodeFailureReason(), decodedIBFDebugger.toString()));
        }

        IbfSyncResult ibfSyncResult;
        if (adapter.hasCompoundPkSupport())
            ibfSyncResult =
                    new IbfSyncResult(
                            diffManager.getResult(),
                            ((IbfTableEncoderWithCompoundPK) adapter).keyTypes(),
                            ((IbfTableEncoderWithCompoundPK) adapter).keyLengths());
        else
            ibfSyncResult =
                    new IbfSyncResult(diffManager.getResult(), adapter.keyType(), adapter.keyLength());

        this.lastRecordCount = ibfSyncResult.upserts().size() + ibfSyncResult.deletes().size();
        return ibfSyncResult;
    }

    /**
     * Apply the changes from the latest result to the persisted IBF in order to sync it with the current state of the
     * database table
     *
     * <p>The update process is:
     *
     * <ol>
     *   <li>If we are performing a replacement: persist the replacement IBF to cloud storage and we're done
     *   <li>Otherwise, load the starting-sized persistedIBF from disk into memory
     *   <li>Update the persistedIBF to the latest state by inserting or removing the elements recovered in the result
     *       of the diff() operation to the persistedIBF
     *   <li>Persist the result of Step 3 to cloud storage
     * </ol>
     *
     * @throws IOException
     */
    public void update() throws IOException {
        if (!diffManager.hasResult()) throw new IllegalStateException("diff() must be called before update()");
        if (!diffManager.compareSucceeded())
            throw new IllegalStateException("cannot update unless diff returned a successful result");

        if (adapter.ifReplacementRequired()) {
            persistIbfSyncData(new IbfSyncData(diffManager.getReplacementIBF(), lastRecordCount));
            updatePerformedReplacement = true;
            return;
        }

//        IbfMetricsLogging.reportIBFSizePerTable(
//                diffManager.previousIBFDataSize(),
//                TagName.TABLE.toString(),
//                adapter.tableRef().toString(),
//                TagName.IBF_TYPE.toString(),
//                IbfSubPhaseTags.PERSISTED_IBF.value());

        persistIbfSyncData(new IbfSyncData(diffManager.updatePreviousIBFWithResult(), lastRecordCount));
    }

    @VisibleForTesting
    public boolean hasPersistedIBF() {
        try {
            return downloadPersistedIBF() != null;
        } catch (IOException | NullPointerException e) {
            return false;
        }
    }

    @VisibleForTesting
    ResizableInvertibleBloomFilter fetchStartingSizedIBF() throws SQLException {
        return adapter.getResizableInvertibleBloomFilter(smallCellCount(), startingSize);
    }

    // fetch latest IBF with default column values for IBF transition to handle ADD_COLUMN schema change
    ResizableInvertibleBloomFilter fetchStartingSizedIBFWithColumnDefaults() throws SQLException {
        return adapter.getReplacementResizableInvertibleBloomFilter(smallCellCount(), startingSize);
    }

    @VisibleForTesting
    ResizableInvertibleBloomFilter fetchSizedIBF(int smallCellCount) throws SQLException {
        return adapter.getResizableInvertibleBloomFilter(smallCellCount, currentSize);
    }

    @VisibleForTesting
    public void simulateResetWithEmptyIBF() {
        ResizableInvertibleBloomFilter emptyIBF;
        if (adapter.hasCompoundPkSupport()) {
            int keyLengthsSum =
                    ((IbfTableEncoderWithCompoundPK) adapter)
                            .keyLengths()
                            .stream()
                            .mapToInt(Integer::intValue)
                            .sum();
            emptyIBF = new ResizableInvertibleBloomFilter(keyLengthsSum, smallCellCount(), startingSize);
        } else emptyIBF = new ResizableInvertibleBloomFilter(adapter.keyLength(), smallCellCount(), startingSize);

        persistIbfSyncData(new IbfSyncData(emptyIBF, 0));
    }

    private void persistIbfSyncData(IbfSyncData syncData) {
        if (debug) return;
        ByteBuf serializedBuf = Unpooled.buffer(syncData.dataSize());
        syncDataSerializer.encode(syncData, serializedBuf);
        try {
            storage.put(objectID, serializedBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try {
//            RETRIER.run(
//                    () ->
//                            runWithDurationReport(
//                                    IbfTimerSampler.IBF_IBF_UPLOAD,
//                                    () -> {
//                                        ByteBuf serializedBuf = Unpooled.buffer(syncData.dataSize());
//                                        syncDataSerializer.encode(syncData, serializedBuf);
//                                        storage.put(objectID, serializedBuf);
//                                        return null;
//                                    }),
//                    MAX_RETRIES);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new RuntimeException(e);
//        }
    }

    @VisibleForTesting
    ResizableInvertibleBloomFilter downloadPersistedIBF() throws IOException {
        try {
            IbfSyncData syncData =
                    (IbfSyncData)
                            runWithDurationReport(
                                    IbfTimerSampler.IBF_IBF_DOWNLOAD,
                                    () -> {
                                        ByteBuf received = storage.get(objectID);
                                        return syncDataSerializer.decode(received);
                                    });
            this.lastRecordCount = syncData.lastRecordCount;
            return syncData.persistedIBF;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private int smallCellCount() {
        try {
            Optional<Long> estimatedRowCount = adapter.estimatedRowCount();

            /** Use default value when estimatedRowCount does returns empty */
            if (!estimatedRowCount.isPresent()) return DEFAULT_SMALL_CELL_COUNT;

            /**
             * Set the SMALL cell count such that the starting size cell count can detect differences in 11.4% of the
             * rows of the table. These parameters were initially chosen so that the number of differences detectable by
             * a SMALL-sized ResizableIBF equals 0.001% of the rows in the table when using the biggest starting size.
             */
            int smallCellCount =
                    Math.toIntExact(
                            (long) Math.ceil(ALPHA * estimatedRowCount.get() * 0.114 / startingSize.cellCountFactor()));

            /** Apply minimum threshold */
            if (!disableMinimumSmallCellCount) smallCellCount = Math.max(smallCellCount, MINIMUM_SMALL_CELL_COUNT);

            /** Apply maximum threshold */
//            smallCellCount = Math.min(smallCellCount, maxSmallCellCount);
            smallCellCount = Math.min(smallCellCount, 1);

            return smallCellCount;
        } catch (ArithmeticException e) {
//            LOG.warning("Fail to compute the small cell count with exception: " + e.getMessage());
//            return maxSmallCellCount;
            return -1;
        }
    }

    private Object runWithDurationReport(IbfTimerSampler sampler, Callable<Object> operation) throws Exception {
        sampler.start(adapter.tableRef());

        Object result = operation.call();

        sampler.stop();
        return result;
    }

    private void runWithDurationReport(IbfTimerSampler sampler, Runnable operation) {
        sampler.start(adapter.tableRef());

        operation.run();

        sampler.stop();
    }

    private ResizableInvertibleBloomFilter.Sizes determineIBFSize(int smallCellCount) {
        if (this.lastRecordCount == 0) return ResizableInvertibleBloomFilter.Sizes.SMALL;
        int estimatedRequiredCellCount = (int) Math.ceil(this.lastRecordCount * ALPHA * ESTIMATE_OVERHEAD);
        for (ResizableInvertibleBloomFilter.Sizes size : ResizableInvertibleBloomFilter.SIZES_ATTEMPT_ORDER) {
            if (size.cellCountFactor() >= startingSize.cellCountFactor()) continue;

            if (estimatedRequiredCellCount < smallCellCount * size.cellCountFactor()) return size;
        }
        return startingSize;
    }

    @VisibleForTesting
    int computeMaxSmallCellCountWithHeapSize() {
        long memToUseInBytes = getMemToUseInBytes();
        int numOfCells = (int) Math.floor(memToUseInBytes / (double) computeCellSizeInBytesInMem());
        int maxSmallCellCount = numOfCells / 114; // STARTING IBF has ~114 * smallCellCount # of cells
//        LOG.info(
//                InfoEvent.of(
//                        "ibf_max_cell_count",
//                        String.format(
//                                "With max heap of %d MB, MAX_SMALL_CELL_COUNT is %s",
//                                Runtime.getRuntime().maxMemory() / 1024 / 1024, maxSmallCellCount)));
        return maxSmallCellCount;
    }

    @VisibleForTesting
    long getMemToUseInBytes() {
        long memoryUpperBound = Math.min(6L * 1024 * 1024 * 1024, Runtime.getRuntime().maxMemory());
        // we use 40% of the max heap as the upper bound for ibf InvertibleBloomFilter
        return (long) Math.floor(memoryUpperBound * 0.3);
    }

    /*
            In Java, JVM will allocate some extra memory for object header and reference when initializing an object.
            For reference, https://www.baeldung.com/java-size-of-object#:~:text=References%20have%20a%20typical%20size,%2D50%25%20more%20heap%20space.
            so approximate cell size in mem = keySums size + rowHash size + count size + keySums object header size
            + cell object header size + cell object reference from InvertibleBloomFilter#cells
    */
    @VisibleForTesting
    int computeCellSizeInBytesInMem() {
        int keyLengthSum;
        if (adapter.hasCompoundPkSupport())
            keyLengthSum =
                    ((IbfTableEncoderWithCompoundPK) adapter)
                            .keyLengths()
                            .stream()
                            .mapToInt(Integer::intValue)
                            .sum();
        else keyLengthSum = adapter.keyLength();
        return keyLengthSum * Long.BYTES + Long.BYTES + Long.BYTES + 16 + 16 + 4;
    }

    class DiffManager {
        private SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> previousIBF;
        private SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> resizedPreviousIBF;
        private SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> latestIBF;
        private SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> replacementIBF;

        private InvertibleBloomFilter diffIBF;

        private IBFDecodeResult result;

        private long resultDataSize;

        DiffManager() {}

        void setPreviousIBF(ResizableInvertibleBloomFilter value) {
            previousIBF = new SecureTempFileStoredIBF<>(value);
        }

        void setLatestIBF(ResizableInvertibleBloomFilter value) {
            latestIBF = new SecureTempFileStoredIBF<>(value);
        }

        void setReplacementIBF(ResizableInvertibleBloomFilter value) {
            replacementIBF = new SecureTempFileStoredIBF<>(value);
        }

        IBFDecodeResult getResult() {
            if (result == null) throw new IllegalStateException("result missing");

            return result;
        }

        ResizableInvertibleBloomFilter getReplacementIBF() {
            if (replacementIBF == null) throw new IllegalStateException("replacement missing");

            return replacementIBF.getIBF();
        }

        int latestIBFDataSize() {
            return latestIBF.getDataSize();
        }

        long resultDataSize() {
            return resultDataSize;
        }

        int previousIBFDataSize() {
            return previousIBF.getDataSize();
        }

        void resizePreviousIBF(ResizableInvertibleBloomFilter.Sizes size) {
            if (size == ResizableInvertibleBloomFilter.XLARGE) return;

            this.resizedPreviousIBF = SecureTempFileStoredIBF.resize(previousIBF, size);
        }

        void compare() {
            if (previousIBF == null && resizedPreviousIBF == null) throw new IllegalStateException("missing persisted");
            if (latestIBF == null) throw new IllegalStateException("missing latest");

            SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> previousIBFToSubtract;
            if (resizedPreviousIBF != null) previousIBFToSubtract = resizedPreviousIBF;
            else previousIBFToSubtract = previousIBF;

            diffIBF = SecureTempFileStoredIBF.subtract(latestIBF, previousIBFToSubtract);

            this.result = diffIBF.decode();
            this.resultDataSize = result.dataSize();

            // Only keep the diffIBF if the decode failed
            if (this.result.succeeded) diffIBF = null;

            clearResizedPreviousIBF();
        }

        boolean compareSucceeded() {
            return hasResult() && result.succeeded;
        }

        boolean hasResult() {
            return result != null;
        }

        ResizableInvertibleBloomFilter updatePreviousIBFWithResult() {
            if (previousIBF == null) throw new IllegalStateException("previous missing");

            ResizableInvertibleBloomFilter previousIBFValue = previousIBF.getIBF();

            for (IBFDecodeResultElement element : result.aWithoutB) {
                previousIBFValue.insert(element.keySum, element.rowHashSum);
            }

            for (IBFDecodeResultElement element : result.bWithoutA) {
                previousIBFValue.remove(element.keySum, element.rowHashSum);
            }

            return previousIBFValue;
        }

        private void clearResizedPreviousIBF() {
            resizedPreviousIBF = null;
        }
    }
}
