package com.example.db;

import com.example.core.TableRef;
import com.example.micrometer.TagValue;

import java.util.Comparator;

/** Defines the method prototypes for classes that sync entire tables for a particular database. */
public interface DbImporter<Table> {

    /**
     * Defines the order in which each connectors imports tables. This method will typically return a comparator of
     * table byte size, which allows us to import tables from smallest to largest.
     */
    Comparator<Table> tableSorter();

    /** Returns true when a table has begun an import and false otherwise. */
    public boolean importStarted(TableRef table);

    /** Returns true when a table has finished importing. */
    boolean importFinished(Table table);

    /** Soft delete all {@link Table} records in the warehouse that were synced before the import began */
    default void modeSpecificDelete(Table table) {
        // TODO remove default definition after implemented in all subclasses
        throw new UnsupportedOperationException();
    }

    /** Returns true if {@link Table} has been soft deleted after the import was finished */
    default boolean softDeletedAfterImport(Table table) {
        // TODO remove default definition after implemented in all subclasses
        throw new UnsupportedOperationException();
    }

    /**
     * Imports the next page from the specified table. Implementations must handle state management to ensure pages are
     * properly ordered.
     */
    void importPage(Table table);

    default void recordImportDuration(TagValue.SyncStatus status, String dbVersion) {
        // no-op for those who haven't implemented it yet
    }

    /** Hook for adding side effects that occur before an import starts * */
    default void beforeImport(Table table) {
        // no-op for those who haven't implemented it yet
    }
}
