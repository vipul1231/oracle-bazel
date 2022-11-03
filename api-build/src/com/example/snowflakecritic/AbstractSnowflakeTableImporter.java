package com.example.snowflakecritic;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.logger.ExampleLogger;
import com.example.utils.ExampleClock;

import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractSnowflakeTableImporter extends AbstractSnowflakeTableSyncer
        implements SnowflakeTableImporter {

    protected static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    protected AbstractSnowflakeTableImporter(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
    }

    protected abstract String getImportMethodName();

    protected abstract String getTableQuery();

    @Override
    public final void importPage() {
        LOG.info("Importing table: " + tableRef);
        tableState.importMethod = getImportMethodName();

        String queryTable = getTableQuery();
        Instant importStarted = ExampleClock.Instant.now();

        try {
            getSnowflakeSource()
                    .execute(
                            connection -> {
                                useSchema(connection);

                                try (Statement queryStatement = connection.createStatement();
                                     ResultSet resultSet = queryStatement.executeQuery(queryTable)) {
                                    ResultSetHelper resultSetHelper = new ResultSetHelper(tableInfo, resultSet);
                                    while (resultSet.next()) {
                                        submitRow(resultSetHelper.extractRowValues());
                                    }
                                }
                            });

            stopImport();
        } catch (Exception e) {
            onImportException(e);
        } finally {
            checkpoint();
            afterImport();
        }
    }

    @Override
    public void onImportException(Exception e) {
        tableState.importFailed();
        //LOG.log("Import for table " + tableRef + " failed", e);
    }

    /**
     * Called at the end of a table import regardless of whether it was successful. Override to perform any post-load
     * cleanup.
     */
    protected void afterImport() {
    }

    /**
     * Called once after startImport but before any calls to importPage.
     */
    protected void beforeImport() {
        Optional<PreTableImportHandler> optionalHandler = serviceConfig.getPreTableImportHandler(tableRef);

        if (optionalHandler.isPresent()) {
            try {
                optionalHandler.get().handlePreImport();
            } catch (Exception e) {
                e.printStackTrace();
                onImportException(e);
            } finally {
                checkpoint();
            }
        }
    }

    @Override
    public final void startImport() {
        LOG.info("Start import: " + tableRef.name);
        beforeImport();
        if (!tableState.isImportFailed()) {
            tableState.importStarted(ExampleClock.Instant.now());
        }
    }

    @Override
    public void stopImport() {
        tableState.importCompleted();
    }

    @Override
    public boolean isImportStarted() {
        return tableState.isImportStarted();
    }

    @Override
    public boolean isTableImportComplete() {
        return tableState.isImportComplete();
    }

    @Override
    protected final void submit(Map<String, Object> row) {
        if (getSyncMode() != null && getSyncMode() == SyncMode.History) {
            getOutput().upsert(tableInfo.tableDefinition(), row, tableState.syncBeginTime, getOperationTags());
        } else {
            getOutput().upsert(tableInfo.tableDefinition(), row, getOperationTags());
        }
    }
}