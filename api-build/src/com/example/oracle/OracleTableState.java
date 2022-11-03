package com.example.oracle;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.utils.ExampleClock;

import java.time.Instant;

/**
 * Encapsulates state related information specific to a single table. Simplifies interacting with the OracleState object
 * and also provides clear enumerated values for initial sync and incremental update process states.
 */
public class OracleTableState {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public enum TableImportState {
        NOT_STARTED,
        IN_PROGRESS,
        COMPLETED,
        UNKNOWN
    }

    public enum TableIncrementalUpdateState {
        NOT_STARTED,
        IN_PROGRESS,
        COMPLETED,
        FAILED,
        UNKNOWN
    }

    private final TableRef table;

    private final OracleState oracleState;

    private TableIncrementalUpdateState incrementalUpdateState = TableIncrementalUpdateState.NOT_STARTED;

    public OracleTableState(OracleState oracleState, TableRef table) {
        this.oracleState = oracleState;
        this.table = table;
    }

    public String getIbfStorageId() {
        return oracleState.getIbfStorageId(table);
    }

    public OracleState getOracleState() {
        return oracleState;
    }

    public TableImportState getImportState() {
        if (oracleState.isBeforeStartingTableImport(table)) {
            return TableImportState.NOT_STARTED;
        } else if (oracleState.isImportInProgress(table)) {
            return TableImportState.IN_PROGRESS;
        } else if (oracleState.tableImportComplete(table)) {
            return TableImportState.COMPLETED;
        }

        // If the OracleState is inconsistent (it shouldn't be, but just in case).
        return TableImportState.UNKNOWN;
    }

    public boolean isInitialSyncCompleted() {
        return getImportState() == TableImportState.COMPLETED;
    }

    public TableIncrementalUpdateState getIncrementalUpdateState() {
        return incrementalUpdateState;
    }

    public boolean isIncrementalSyncCompleted() {
        return incrementalUpdateState == TableIncrementalUpdateState.COMPLETED;
    }

    public void beginIncrementalSync() {
        incrementalUpdateState = TableIncrementalUpdateState.IN_PROGRESS;
    }

    public void failIncrementalSync() {
        incrementalUpdateState = TableIncrementalUpdateState.FAILED;
    }

    public void endIncrementalSync() {
        incrementalUpdateState = TableIncrementalUpdateState.COMPLETED;
    }

    public boolean isPreImport() {
        return getImportState() == TableImportState.NOT_STARTED;
    }

    public void reset() {
        oracleState.resetTable(table);
    }

    public void recordSyncStartTime() {
        oracleState.syncBeginTime.put(table, ExampleClock.Instant.now());
    }

    public Instant getSyncStartTime() {
        return oracleState.syncBeginTime.get(table);
    }
}