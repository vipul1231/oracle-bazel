package com.example.oracle;

import com.example.core.TableRef;

/** */
public interface IbfTableWorker {

    /**
     * Perform any work related to the table if it has not yet been imported. This object knows its own state so it will
     * only perform work if the table is in the pre import state.
     */
    void performPreImport();

    /** Most incremental sync mechanisms don't differentiate by table (i.e. LogMiner) but ibf does. */
    void performIncrementalSync();

    /**
     * Validates that the table can be imported/updated.
     *
     * @throws DbObjectNotSupportedException
     */
    void validateTable() throws Exception;

    TableRef getTableRef();
}
