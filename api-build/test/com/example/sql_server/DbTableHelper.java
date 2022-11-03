package com.example.sql_server;

import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class DbTableHelper<Table> {

    /**
//     * Defines the standard naming format for test tables used by our Dockerized databases and {@link RecordComparator}.
//     * The table names cannot be random since {@link RecordComparator} compares actual table output from {@link
     * MockOutput2} with expected table output from hard-coded json files.
     */
    public abstract Table defaultTable();

    /**
     * Use when multiple tables are required in a single test.
     *
     * @param customTableName
     */
    public abstract Table customTable(String customTableName);

    public abstract TableRef destinationTableRef(Table table, com.example.core2.ConnectionParameters params);

    public StandardConfig createStandardConfig(Table tableToUpdate) {
        return createStandardConfig(ImmutableList.of(tableToUpdate), SyncMode.Legacy);
    }

    public StandardConfig createStandardConfig(Table tableToUpdate, SyncMode syncMode) {
        return createStandardConfig(ImmutableList.of(tableToUpdate), syncMode);
    }

    public StandardConfig createStandardConfig(Collection<Table> tablesToUpdate) {
        return createStandardConfig(tablesToUpdate, SyncMode.Legacy);
    }

    public StandardConfig createStandardConfig(Collection<Table> tablesToUpdate, SyncMode syncMode) {
        Map<Table, SyncMode> tableModes = tablesToUpdate.stream().collect(Collectors.toMap(t -> t, m -> syncMode));
        return createStandardConfig(tableModes);
    }

    public StandardConfig createStandardConfig(Map<Table, SyncMode> tablesToUpdate) {
        throw new UnsupportedOperationException("Sync mode not supported for this integration.");
    }
}
