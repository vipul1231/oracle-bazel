package com.example.snowflakecritic;

import com.example.core.Column;
import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.example.core2.Output;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.ibf.schema.SchemaChangeResult;
import com.example.ibf.schema.SchemaChangeType;
import com.example.logger.ExampleLogger;
import com.example.snowflakecritic.ibf.SnowflakeIbfAdapter;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SnowflakeIbfTableUpdater extends AbstractSnowflakeTableUpdater {

    private IbfCheckpointManager checkpointManager;
    private final SnowflakeIbfSchemaManager schemaManager;
    private static final int PAGE_LIMIT = 16348;

    Logger LOG = Logger.getLogger(SnowflakeIbfTableUpdater.class.getName());
    private Output<SnowflakeConnectorState> output;


    public SnowflakeIbfTableUpdater(
            SnowflakeConnectorServiceConfiguration serviceConfig,
            SnowflakeIbfSchemaManager schemaManager,
            TableRef tableRef,
            IbfCheckpointManager<SnowflakeIbfAdapter> ibfCheckpointManager) {
        super(serviceConfig, tableRef);
        this.schemaManager = schemaManager;
        this.checkpointManager = ibfCheckpointManager;

    }

    @Override
    public SnowflakeSourceCredentials.UpdateMethod getUpdateMethod() {
        return SnowflakeSourceCredentials.UpdateMethod.IBF;
    }


    @Override
    protected OperationTag[] getOperationTags() {
        return new OperationTag[] {OperationTag.INCREMENTAL_SYNC, OperationTag.IBF};
    }

    @Override
    protected void doPerformIncrementalUpdate() {
        ExampleLogger.getMainLogger().info("incrementalUpdate " + tableRef);

        SchemaChangeResult result = schemaManager.getSchemaChange(tableRef);

        if (result.type == SchemaChangeType.UNKNOWN_CHANGE) {
            // error
            // IbfSchemaManager doesn't currently return this type
//            throw new Exception(
//                    "Unknown schema change - "
//                            + result.type
//                            + " detected for "
//                            + tableRef
//                            + ": skipping incremental update.");
        } else if (result.isResyncNeeded) {
            resyncTable(result.type.getDescription() + ": " + tableRef);
        } else {
            if (result.type != SchemaChangeType.NO_CHANGE) {
                // To be implemented in Beta: schema change handling.
                resyncTable("Schema Changes not supported in Private Preview: " + tableRef);
            } else {
                updateTable();
            }
        }
    }

    private void updateTable() {
        IbfSyncResult result = null;
        try {
            result = checkpointManager.diff();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!result.getSucceeded()) {
            System.out.println("not succeed in getting diff");
        }

        LOG.info("Processing changes for table " + tableRef.name);
        processUpserts(result);
        processDeletes(result);

        try {
            checkpointManager.update();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected String getTimeForLastSync() {
        return ExampleClock.Instant.now().toString();
    }

    @Override
    public void performPreImport() {
        try {
            this.checkpointManager.reset();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            checkpoint();
        }
    }

    public IbfCheckpointManager checkpointManager() {
        return checkpointManager;
    }

    @Override
    public void incrementalUpdate() {
        LOG.info("incrementalUpdate " + tableRef);
        try {
            tableState.updateStarted(Instant.now());
            IbfSyncResult result = checkpointManager.diff();
            if (!result.getSucceeded()) {
                LOG.info("Ibf diff did not succeed for table " + tableRef.name);
                tableState.updateFailed();
                return;
            }
            LOG.info("Processing changes for table " + tableRef.name);
            processUpserts(result);
            processDeletes(result);
            checkpointManager.update();
            tableState.updateCompleted();
        } catch (Exception e) {
            e.printStackTrace();
            tableState.updateFailed();
        }
    }


    private void processUpserts(IbfSyncResult result) {
        if (result.upserts().isEmpty()) return;

        SnowflakeTableInfo tableInfo = getSnowflakeInformer().tableInfo(tableRef);
        Iterator<List<Object>> allUpsertKeys = result.upserts().iterator();

        while (allUpsertKeys.hasNext()) {
            List<List<Object>> upsertKeysSubSet = ImmutableList.copyOf(Iterators.limit(allUpsertKeys, PAGE_LIMIT));
            importPageOfUpserts(tableInfo, upsertKeysSubSet);
        }

        checkpoint();
    }

    private void importPageOfUpserts(SnowflakeTableInfo tableInfo, List<List<Object>> upsertKeyValues) {
        try {
            getSnowflakeSource()
                    .execute(
                            connection -> {
                                selectRowsAndImport(connection, tableInfo, upsertKeyValues);
                            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            checkpoint();
        }
    }

    private void selectRowsAndImport(
            Connection connection, SnowflakeTableInfo tableInfo, List<List<Object>> upsertKeyValues)
            throws SQLException {
        // select part
        String selectQuery =
                SnowflakeSQLUtils.select(
                        getSnowflakeInformer().tableInfo(tableRef).includedColumnInfo(), tableRef.name);

        // get the key list in the where clause
        List<String> keyNames = tableInfo.primaryKeys().stream().map(c -> c.columnName).collect(Collectors.toList());
        selectQuery += SnowflakeSQLUtils.whereIn(keyNames, upsertKeyValues);

        try (PreparedStatement statement = connection.prepareStatement(selectQuery);
             ResultSet rows = statement.executeQuery()) {
            ResultSetHelper resultSetHelper = new ResultSetHelper(tableInfo, rows);
            while (rows.next()) {
                submitRow(resultSetHelper.extractRowValues());
            }
        }
    }


    private void processDeletes(IbfSyncResult result) {
        List<SnowflakeColumnInfo> sortedKeyColumns = tableInfo.primaryKeys();
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
                values.put(sortedKeyColumns.get(i).columnName, keys.get(i));
            }
            delete(values, tableState.syncBeginTime);
        }

        checkpoint();
    }

    protected void delete(Map<String, Object> key, Instant changeTime) {
        Map<String, Object> exampleRow = new HashMap<>(key);
        SnowflakeTypeCoercer typeCoercer = new SnowflakeTypeCoercer();
        tableInfo
                .primaryKeys()
                .forEach(
                        columnInfo -> {
                            exampleRow.put(
                                    columnInfo.columnName,
                                    typeCoercer.minimize(key.get(columnInfo.columnName), columnInfo.sourceType));
                        });
        deleteRecord(exampleRow, changeTime);
    }
}

