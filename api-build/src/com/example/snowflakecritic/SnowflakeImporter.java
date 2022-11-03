package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.db.DbImporter;
import com.example.logger.ExampleLogger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class SnowflakeImporter extends SnowflakeConfigurationAware implements DbImporter<TableRef> {

    private Map<TableRef, SnowflakeTableImporter> tableImporters = new HashMap<>();

    public SnowflakeImporter(SnowflakeConnectorServiceConfiguration config) {
        super(config);
    }

    @Override
    public Comparator<TableRef> tableSorter() {
        return Comparator.comparingLong(
                t -> serviceConfig.getSnowflakeInformer().tableInfo(t).estimatedDataBytes.orElse(0L));
    }

    @Override
    public boolean importStarted(TableRef tableRef) {
        return getSnowflakeTableImporter(tableRef).isImportStarted();
    }

    @Override
    public boolean importFinished(TableRef tableRef) {
        return getSnowflakeTableImporter(tableRef).isTableImportComplete();
    }

    @Override
    public void beforeImport(TableRef tableRef) {
        getSnowflakeTableImporter(tableRef).startImport();
    }

    @Override
    public void importPage(TableRef tableRef) {
        getSnowflakeTableImporter(tableRef).importPage();
    }

    @Override
    public boolean softDeletedAfterImport(TableRef tableRef) {
        ExampleLogger.getMainLogger().info("softDeletedAfterImport " + tableRef);
        return false;
    }

    @Override
    public void modeSpecificDelete(TableRef tableRef) {
        ExampleLogger.getMainLogger().info("modeSpecificDelete " + tableRef);
    }

    protected SnowflakeTableImporter getSnowflakeTableImporter(TableRef tableRef) {
        return tableImporters.computeIfAbsent(tableRef, serviceConfig::newSnowflakeTableImporter);
    }
}
