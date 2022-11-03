package com.example.db;

import com.example.core.TableRef;
import com.example.micrometer.TagValue;

import java.util.Set;

/**
 * Defines the method prototypes for classes that sync all the changes made to a particular database.
 *
 * <p>{@see DbSyncer, DbImporter}
 */
public interface DbIncrementalUpdater {

    void incrementalUpdate(Set<TableRef> includedTables);

    default void recordUpdateDuration(TagValue.SyncStatus syncStatus, String dbVersion) {
        // no-op for non-implementers
    }
}
