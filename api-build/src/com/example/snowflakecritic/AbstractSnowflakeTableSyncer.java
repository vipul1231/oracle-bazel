package com.example.snowflakecritic;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.example.oracle.Names;

import java.time.Instant;
import java.util.Map;

public abstract class AbstractSnowflakeTableSyncer extends AbstractSnowflakeTableWorker {
    public AbstractSnowflakeTableSyncer(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
    }

    /** @param row */
    protected final void submitRow(Map<String, Object> row) {
        // Process row first, then get TableDefinition: the TableDefinition
        // might change due to type promotion during handleTypePromotions.
        submit(handleTypePromotions(row));
    }

    protected void deleteRecord(Map<String, Object> keys, Instant changeTime) {

        switch (getSyncMode()) {
            case History:
                getOutput().delete(tableInfo.tableDefinition(), keys, changeTime);
                break;
            default: // Legacy
                keys.put(Names.example_DELETED_COLUMN, true);
                getOutput().update(tableInfo.tableDefinition(), keys);
        }
    }

    protected SyncMode getSyncMode() {
        return getStandardConfig().syncModes().get(tableRef);
    }

    protected abstract void submit(Map<String, Object> row);

    protected void resyncTable(String s) {}

    protected abstract OperationTag[] getOperationTags();
}
