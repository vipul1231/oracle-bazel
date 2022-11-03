package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbIncrementalUpdater;
import com.example.utils.ExampleClock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SnowflakeIncrementalUpdater extends SnowflakeConfigurationAware implements DbIncrementalUpdater {

    private Map<TableRef, SnowflakeTableUpdater> tableUpdaters = new HashMap<>();

    public SnowflakeIncrementalUpdater(SnowflakeConnectorServiceConfiguration config) {
        super(config);
    }

    @Override
    public void incrementalUpdate(Set<TableRef> includedTables) {

        for (TableRef tableRef : includedTables) {
            SnowflakeConnectorState.SnowflakeTableState tableState =
                    getSnowflakeConnectorState().getTableState(tableRef);
            SnowflakeTableUpdater updater = getSnowflakeTableUpdater(tableRef);
            tableState.updateMethod = updater.getUpdateMethod();
            if (tableState.isPreImport()) {
                updater.performPreImport();
            }

            if (updater.canUpdate()) {
                if (!tableState.isUpdateStarted()) {
                    tableState.updateStarted(ExampleClock.Instant.now());
                }

                try {
                    updater.incrementalUpdate();
                } catch (Exception ex) {
                    //ExampleClock.getMainLogger().severe("Incremental update of table " + tableRef + " failed.", ex);
                    tableState.updateFailed();
                }
            }
        }
    }

    public SnowflakeTableUpdater getSnowflakeTableUpdater(TableRef tableRef) {
        return tableUpdaters.computeIfAbsent(tableRef, serviceConfig::newSnowflakeTableUpdater);
    }

}
