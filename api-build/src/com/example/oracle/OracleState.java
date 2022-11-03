package com.example.oracle;

import com.example.core.TableRef;
import com.example.flag.FlagName;
import com.fasterxml.jackson.annotation.JsonAlias;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/28/2021<br/>
 * Time: 11:01 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleState {

    public Optional<Long> earliestUncommitedScn = Optional.empty();
    public Optional<Long> dynamicPageSize = Optional.empty();
    public Map<TableRef, Long> perTableSyncedScn = new ConcurrentHashMap<>();
    public Optional<Long> nextReadScn = Optional.empty();
    public String storageId;

    // Ibf state
    public Map<TableRef, String> ibfObjectIds = new HashMap<>();

    /** Keeps track of which tables have completed their initial sync. */
    public final Set<TableRef> initialSyncComplete = new HashSet<>();

    public Map<TableRef, BlockRange> remainingBlockRange = new HashMap<>();

    public Map<TableRef, Instant> syncBeginTime = new HashMap<>();
    /** a list of tables that haven't had update during the previous cycle. */
    public Set<TableRef> inactiveTables = new HashSet<>();

    public void resetTable(TableRef table) {
        initialSyncComplete.remove(table);
        remainingBlockRange.remove(table);
        syncBeginTime.remove(table);
        perTableSyncedScn.remove(table);
        inactiveTables.remove(table);
    }

    public void resetAll() {
        earliestUncommitedScn = Optional.empty();
        dynamicPageSize = Optional.empty();
        perTableSyncedScn.clear();
        initialSyncComplete.clear();
        remainingBlockRange.clear();
        syncBeginTime.clear();
        inactiveTables.clear();
    }

    public boolean isBeforeStartingTableImport(TableRef tableRef) {
        if (FlagName.OracleSkipIncrementalSyncBeforeImportStarts.check()) {
            return getTableSyncedScn(tableRef) == null;
        } else {
            return !initialSyncComplete.contains(tableRef) && !remainingBlockRange.containsKey(tableRef);
        }
    }

    public boolean isImportInProgress(TableRef tableRef) {
        return !initialSyncComplete.contains(tableRef) && remainingBlockRange.containsKey(tableRef);
    }

    public boolean tableImportComplete(TableRef tableRef) {
        return initialSyncComplete.contains(tableRef);
    }


    /** active table means there has been change in this table during the previous sync */
    public boolean wasTableUpdated(TableRef tableRef) {
        return !inactiveTables.contains(tableRef);
    }

    public void setWasTableUpdated(TableRef table, boolean updated) {
        if (!updated) inactiveTables.add(table);
        else {
            inactiveTables.remove(table);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OracleState that = (OracleState) o;
        return Objects.equals(earliestUncommitedScn, that.earliestUncommitedScn)
                && Objects.equals(initialSyncComplete, that.initialSyncComplete)
                && Objects.equals(remainingBlockRange, that.remainingBlockRange)
                && Objects.equals(syncBeginTime, that.syncBeginTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(earliestUncommitedScn, initialSyncComplete, remainingBlockRange, syncBeginTime);
    }

    void setTableSyncedScn(TableRef table, Long syncedScn) {
        perTableSyncedScn.put(table, syncedScn);
    }

    public Long getTableSyncedScn(TableRef table) {
        return perTableSyncedScn.get(table);
    }

    /**
     * If we've synced this table previously, return the SCN corresponding to the last-synced changes. If we haven't,
     * store the current SCN as last-synced, and return that.
     */
    public Long initTableSyncedScn(TableRef table, Long currentScn) {
        if (perTableSyncedScn.containsKey(table)) {
            return perTableSyncedScn.get(table);
        } else {
            perTableSyncedScn.put(table, currentScn);
            return currentScn;
        }
    }

    public String getIbfStorageId(TableRef tableRef) {
        if (null == ibfObjectIds) {
            ibfObjectIds = new HashMap<>();
        }

        return ibfObjectIds.computeIfAbsent(tableRef, t -> UUID.randomUUID().toString());
    }

    public String initializeStorageId() {
        if (null == storageId) {
            this.storageId = UUID.randomUUID().toString();
        }

        return storageId;
    }

    @JsonAlias("tridentStorageId")
    public void setStorageId(String storageId) {
        if (null != storageId) {
            this.storageId = storageId;
        }
    }

    public String getStorageId() {
        return storageId;
    }

}