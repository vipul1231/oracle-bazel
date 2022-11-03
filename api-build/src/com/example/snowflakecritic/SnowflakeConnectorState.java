package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.oracle.OracleState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.example.snowflakecritic.SnowflakeTableSyncStatus.*;

public class SnowflakeConnectorState extends OracleState {
    public final Map<TableRef, SnowflakeTableState> tableStates = new HashMap<>();

    // Set when the configuration tracker is run
    public Instant lastConfigsFetchTime = Instant.EPOCH;

    public byte[] encryptedKey;

    public static class SnowflakeTableState {
        // Based on ExampleClock.Instant.now()
        public Instant syncBeginTime;

        // When used with importMethod and updateMethod TIME_TRAVEL this is the snowflake system time.
        // Otherwise it is set from ExampleClock.Instant.now().
        public String lastSyncTime;

        // Used with the updateMethod IBF only
        private String ibfObjectId;

        public String importMethod;
        public SnowflakeSourceCredentials.UpdateMethod updateMethod;

        // Import. a.k.a. historical import, initial import
        public SnowflakeTableSyncStatus importStatus = NOT_STARTED;

        /** This field is "transient": we don't need to refer to it across syncs */
        @JsonIgnore public SnowflakeTableSyncStatus incUpdateStatus = NOT_STARTED;

        public void importStarted(Instant start) {
            this.syncBeginTime = start;
            this.importStatus = IN_PROGRESS;
        }

        public void importCompleted() {
            this.importStatus = COMPLETE;
        }

        public void importFailed() {
            this.importStatus = FAILED;
        }

        public void updateStarted(Instant start) {
            this.syncBeginTime = start;
            this.incUpdateStatus = IN_PROGRESS;
        }

        public void updateCompleted() {
            this.incUpdateStatus = COMPLETE;
        }

        public void updateFailed() {
            this.incUpdateStatus = FAILED;
        }

        public void reset() {
            this.importStatus = NOT_STARTED;
            this.incUpdateStatus = NOT_STARTED;
            this.syncBeginTime = null;
            this.lastSyncTime = null;
            this.importMethod = null;
            this.updateMethod = null;
        }

        @JsonIgnore
        public boolean isImportFailed() {
            return FAILED == importStatus;
        }

        @JsonIgnore
        public boolean isPreImport() {
            return NOT_STARTED == importStatus;
        }

        @JsonIgnore
        public boolean isImportStarted() {
            return IN_PROGRESS == importStatus;
        }

        @JsonIgnore
        public boolean isImportComplete() {
            return COMPLETE == importStatus;
        }

        @JsonIgnore
        public boolean isUpdateStarted() {
            return IN_PROGRESS == incUpdateStatus;
        }

        @JsonIgnore
        public boolean isUpdateComplete() {
            return COMPLETE == incUpdateStatus;
        }

        public String getIbfObjectId() {
            if (ibfObjectId == null) {
                ibfObjectId = UUID.randomUUID().toString();
            }
            return ibfObjectId;
        }

        public void setIbfObjectId(String ibfObjectId) {
            this.ibfObjectId = ibfObjectId;
        }
    }

    public boolean isTableImportStarted(TableRef tableRef) {
        return tableStates.containsKey(tableRef) && tableStates.get(tableRef).isImportStarted();
    }

    public boolean isTableImportComplete(TableRef tableRef) {
        return tableStates.containsKey(tableRef) && tableStates.get(tableRef).isImportComplete();
    }

    public boolean isTableUpdateStarted(TableRef tableRef) {
        return tableStates.containsKey(tableRef) && tableStates.get(tableRef).isUpdateStarted();
    }

    public boolean isTableUpdateComplete(TableRef tableRef) {
        return tableStates.containsKey(tableRef) && tableStates.get(tableRef).isUpdateComplete();
    }

    public SnowflakeTableState getTableState(TableRef table) {
        return tableStates.computeIfAbsent(table, tableRef -> new SnowflakeTableState());
    }
}
