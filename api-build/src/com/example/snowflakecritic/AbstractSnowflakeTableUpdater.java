package com.example.snowflakecritic;

import com.example.core.SyncMode;
import com.example.core.TableRef;

import java.util.Map;

public abstract class AbstractSnowflakeTableUpdater extends AbstractSnowflakeTableSyncer
        implements SnowflakeTableUpdater {

    protected AbstractSnowflakeTableUpdater(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
    }

    @Override
    public boolean canUpdate() {
        return tableState.isImportComplete() && !tableState.isUpdateComplete();
    }

    @Override
    public void incrementalUpdate() {

        try {
            doPerformIncrementalUpdate();

            tableState.lastSyncTime = getTimeForLastSync();
            tableState.updateCompleted();

            checkpoint();
        } catch (Exception ex) {
            tableState.updateFailed();
            //LOG.severe(String.format("Incremental update for table %s failed", tableRef), ex);
        }
    }

    protected abstract void doPerformIncrementalUpdate();

//    protected void updateSchema(UpdateSchemaOperation schemaOperation) {
//        tableInfo.updateTableDefinition();
//        getOutput().updateSchema(tableInfo.tableDefinition(), schemaOperation);
//    }

    protected abstract String getTimeForLastSync();

    @Override
    protected void submit(Map<String, Object> row) {
        if (getSyncMode() == SyncMode.History) {
            getOutput().upsert(tableInfo.tableDefinition(), row, tableState.syncBeginTime, getOperationTags());
        } else {
            getOutput().upsert(tableInfo.tableDefinition(), row, getOperationTags());
        }
    }
}
