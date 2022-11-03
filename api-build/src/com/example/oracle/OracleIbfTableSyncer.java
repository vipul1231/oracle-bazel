package com.example.oracle;

import com.example.core.Column;
import com.example.core.SyncMode;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.ibf.schema.IbfColumnInfo;
import com.example.ibf.schema.SchemaChangeResult;
import com.example.ibf.schema.SchemaChangeType;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OracleIbfTableSyncer extends OracleTableSyncer implements IbfTableWorker{
    protected static final int DEFAULT_QUERY_TIME_OUT_SECS = 30 * 60; // 30 mins

    private final OracleIbfTableInfo ibfTableInfo;
    private final OracleIbfSchemaChangeManager schemaChangeManager;
    private final IbfCheckpointManager<OracleIbfAdapter> ibfCheckpointManager;
    private final TableQueryBuilder queryBuilder;

    public OracleIbfTableSyncer(
            OracleApi oracleApi,
            OracleTableState tableState,
            OracleIbfTableInfo ibfTableInfo,
            OracleIbfSchemaChangeManager schemaChangeManager,
            IbfCheckpointManager<OracleIbfAdapter> ibfCheckpointManager,
            DataSource dataSource,
            OracleOutputHelper outputHelper,
            DbObjectValidator dbObjectValidator) {
        super(
                oracleApi,
                tableState,
                ibfTableInfo.getOracleTableInfo(),
                dataSource,
                outputHelper,
                dbObjectValidator);
        this.queryBuilder = new TableQueryBuilder(ibfTableInfo.getOracleTableInfo());
        this.ibfTableInfo = ibfTableInfo;
        this.ibfCheckpointManager = ibfCheckpointManager;
        this.schemaChangeManager = schemaChangeManager;
    }

    @Override
    public void performPreImport() {
        // If this table has not yet been imported, then create a baseline IBF.
        if (!tableInfo.isExcluded() && tableState.isPreImport()) {
            try {
                LOG.info("Creating baseline IBF for table " + tableInfo.getTableRef());
                ibfCheckpointManager.reset();
            } catch (SQLException sqlException) {
                throw new RuntimeException("Error calling IbfCheckpointManager#reset.", sqlException);
            } finally {
                checkpoint();
            }
        }
    }

    @Override
    public void performIncrementalSync() {
        if (tableInfo.isExcluded()) {
            LOG.info("Skipping excluded table " + tableInfo.getTableRef());
            return;
        }

        tableState.beginIncrementalSync();

        SchemaChangeResult result = schemaChangeManager.getSchemaChange(ibfTableInfo.getTableRef());

        if (result.type == SchemaChangeType.UNKNOWN_CHANGE) {
            // IbfSchemaManager doesn't currently return this type, so basically there is nothing to do
            // but in the event that it does start returning this we will at least be able to investigate after seeing
            // such a warning in the logs
            LOG.warning("Unknown schema change - " + result.type + " detected for " + tableInfo.getTableRef());
        } else if (result.isResyncNeeded) {
            resyncTable(result.type.getDescription() + ": " + ibfTableInfo.getTableRef());
        } else {
            if (result.type != SchemaChangeType.NO_CHANGE) {
                LOG.info(
                        String.format(
                                "Schema change (%s) detected for table %s",
                                result.type.getDescription(), tableInfo.getTableRef()));
                ibfTableInfo.setModifiedColumns(result.modifiedColumns);
                if (result.type == SchemaChangeType.ADD_COLUMN) {
                    /**
                    for (Map.Entry<String, IbfColumnInfo> entry : result.modifiedColumns.entrySet()) {
                        AddColumnOperation operation = null;
                        Column column =
                                ibfTableInfo
                                        .getOracleTableInfo()
                                        .getColumnInfo(entry.getValue().columnName)
                                        .getTargetColumn();
                        if (tableInfo.getSyncMode() == SyncMode.History) {
                            if (null != entry.getValue().getColumnDefaultValue()) {
                                operation =
                                        new AddColumnWithDefaultValueHistoryModeOperation(
                                                tableInfo.getTableRef(),
                                                column.name,
                                                column.asColumnType(),
                                                entry.getValue().getColumnDefaultValue(),
                                                ExampleClock.Instant.now());
                            } else {
                                operation =
                                        new AddColumnHistoryModeOperation(
                                                tableInfo.getTableRef(),
                                                column.name,
                                                column.asColumnType(),
                                                ExampleClock.Instant.now());
                            }
                        } else if (tableInfo.getSyncMode() == SyncMode.Live) {
                            if (null != entry.getValue().getColumnDefaultValue()) {
                                operation =
                                        new AddColumnWithDefaultValueLiveModeOperation(
                                                tableInfo.getTableRef(),
                                                column.name,
                                                column.asColumnType(),
                                                entry.getValue().getColumnDefaultValue());
                            } else {
                                operation =
                                        new AddColumnLiveModeOperation(
                                                tableInfo.getTableRef(), column.name, column.asColumnType());
                            }
                        }
                        if (operation != null) {
                            outputHelper.updateSchema(tableInfo.getTableRef(), operation);
                        } else {
                            LOG.warning("Add column operation not performed for column " + column.name);
                        }
                     **/
                    }
                }
            }

            updateTable();

    }

    private void updateTable() {
        syncTime = ExampleClock.Instant.now();

        try {
            IbfSyncResult result = ibfCheckpointManager.diff();
            if (!result.getSucceeded()) {
                LOG.warning("Ibf diff did not succeed for table " + tableInfo.getTableRef());
                tableState.failIncrementalSync();
                resyncTable(
                        String.format(
                                "Failed to decode IBF result for table [%s] so we need to resync it",
                                tableInfo.getTableRef()));
            } else {
                LOG.info("Processing changes for table " + tableInfo.getTableRef());
                processUpserts(result);
                processDeletes(result);
                ibfCheckpointManager.update();
                tableState.endIncrementalSync();
            }
        } catch (Exception e) {
            tableState.failIncrementalSync();
            resyncTable(
                    String.format(
                            "IBF for table [%s] is missing from storage so we need to resync it",
                            tableInfo.getTableRef()));
        }
    }

    private void processUpserts(IbfSyncResult result) throws SQLException {
        if (result.upserts().isEmpty()) return;

        int pageLimit = tableInfo.getPageLimit();
        int totalUpsertSize = result.upserts().size();
        int upsertCount = 0;
        Iterator<List<Object>> allUpsertKeys = result.upserts().iterator();

        while (upsertCount < totalUpsertSize) {
            List<List<Object>> upsertKeysSubSet = ImmutableList.copyOf(Iterators.limit(allUpsertKeys, pageLimit));
            importPageOfUpserts(upsertKeysSubSet);
            upsertCount += upsertKeysSubSet.size();
        }
    }

    private void importPageOfUpserts(List<List<Object>> upsertKeyValues) throws SQLException {
        //dataSourceRetrier.runWithConnection(connection -> selectRowsAndImport(connection, upsertKeyValues));

        checkpoint();
    }

    private void selectRowsAndImport(Connection connection, List<List<Object>> upsertKeyValues) throws SQLException {

        String query = queryBuilder.createSelectRowsQuery(upsertKeyValues);
        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet rows = statement.executeQuery()) {

            processRows(rows);
        }
    }

    private void processDeletes(IbfSyncResult result) {
        List<OracleColumnInfo> sortedKeyColumns = tableInfo.getPrimaryKeys();

        for (List<Object> keys : result.deletes()) {
            if (keys.size() != sortedKeyColumns.size()) {
                throw new IllegalArgumentException(
                        "Number of actual keys ("
                                + keys.size()
                                + ") doesn't match number of key columns ("
                                + sortedKeyColumns.size()
                                + ")");
            }
            Map<String, Object> values = new HashMap<>();
            for (int i = 0; i < sortedKeyColumns.size(); i++) {
                values.put(sortedKeyColumns.get(i).getName(), keys.get(i));
            }
            delete(values, syncTime);
        }
    }
}
