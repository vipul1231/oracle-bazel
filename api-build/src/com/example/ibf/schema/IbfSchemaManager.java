package com.example.ibf.schema;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.ibf.exception.DataNotFoundException;
import com.example.logger.ExampleLogger;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IbfSchemaManager {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static final IbfTableInfoSerializer SERIALIZER = new IbfTableInfoSerializer();

    private final String objectId;
    private final IbfPersistentStorage storage;

    public IbfSchemaManager(IbfPersistentStorage storage, String objectId) {
        this.objectId = objectId;
        this.storage = storage;
    }

    public Map<TableRef, SchemaChangeResult> checkAndSaveSchemaChanges(Set<IbfTableInfo> latestTableInfos) {
        Map<TableRef, IbfTableInfo> tableInfosFromStorage;
        Map<TableRef, SchemaChangeResult> updatedTables = new HashMap<>();
        try {
            tableInfosFromStorage = fetch();
        } catch (DataNotFoundException e) {
            LOG.warning("Cannot find the saved schema: " + e.getMessage());
            save(latestTableInfos);
            return updatedTables;
        } catch (IOException e) {
            LOG.warning("Unable to fetch saved schema info from storage: " + e.getMessage());
            return updatedTables;
        }

        for (IbfTableInfo latestTableInfo : latestTableInfos) {
            // new table
            if (!tableInfosFromStorage.containsKey(latestTableInfo.tableRef)) continue;

            IbfTableInfo oldTableInfo = tableInfosFromStorage.get(latestTableInfo.tableRef);
            Map<String, IbfColumnInfo> changedColumns = new HashMap<>();

            /*
               This should be enough for now. In the future, we may need to check the other types of changes when we
               detect one change. For example, customers can add a new column and change a column type at the same time.
            */
            if (latestTableInfo.columns.size() > oldTableInfo.columns.size()) {
                boolean isResyncNeeded = false;
                for (Map.Entry<String, IbfColumnInfo> column : latestTableInfo.columns.entrySet()) {
                    if (oldTableInfo.columns.containsKey(column.getKey())) {
                        continue;
                    }
                    if ((isResyncNeeded =
                            isDynamicColumnDefaultValue(
                                    column.getValue().destColumnType, column.getValue().columnDefaultValue))) break;
                    changedColumns.put(column.getKey(), column.getValue());
                }
                updatedTables.put(
                        latestTableInfo.tableRef,
                        new SchemaChangeResult(SchemaChangeType.ADD_COLUMN, changedColumns, isResyncNeeded));
            } else if (latestTableInfo.columns.size() < oldTableInfo.columns.size()) {
                for (Map.Entry<String, IbfColumnInfo> column : oldTableInfo.columns.entrySet()) {
                    if (latestTableInfo.columns.containsKey(column.getKey())) {
                        continue;
                    }
                    changedColumns.put(column.getKey(), column.getValue());
                }
                updatedTables.put(
                        latestTableInfo.tableRef, new SchemaChangeResult(SchemaChangeType.DROP_COLUMN, changedColumns));
            } else {
                for (Map.Entry<String, IbfColumnInfo> column : latestTableInfo.columns.entrySet()) {
                    if (oldTableInfo.columns.containsKey(column.getKey())) {
                        DataType oldType = oldTableInfo.columns.get(column.getKey()).destColumnType;
                        DataType newType = latestTableInfo.columns.get(column.getKey()).destColumnType;
                        if (oldType != newType && !isSupportedTypePromotion(oldType, newType)) {
                            updatedTables.put(
                                    latestTableInfo.tableRef,
                                    new SchemaChangeResult(SchemaChangeType.CHANGE_COLUMN_TYPE, changedColumns));
                            break;
                        }
                    } else {
                        // we assume the column was renamed. There is no way we can confirm it.
                        updatedTables.put(
                                latestTableInfo.tableRef,
                                new SchemaChangeResult(SchemaChangeType.RENAME_COLUMN, changedColumns));
                        break;
                    }
                }
            }
        }
        save(latestTableInfos);
        return updatedTables;
    }

    @VisibleForTesting
    void save(Set<IbfTableInfo> tableInfos) {
        if (IbfCheckpointManager.debug) return;
        ByteBuf buffer = Unpooled.buffer();
        for (IbfTableInfo tableInfo : tableInfos) {
            SERIALIZER.encode(tableInfo, buffer);
        }
        try {
            storage.put(objectId, buffer);
        } catch (IOException e) {
            LOG.warning("Unable to save latest schema info to storage: " + e.getMessage());
        }
    }

    private Map<TableRef, IbfTableInfo> fetch() throws IOException {
        Map<TableRef, IbfTableInfo> ibfTableInfos = new HashMap<>();
        ByteBuf buffer = storage.get(objectId);
        while (buffer.isReadable()) {
            IbfTableInfo tableInfo = SERIALIZER.decode(buffer);
            ibfTableInfos.put(tableInfo.tableRef, tableInfo);
        }
        return ibfTableInfos;
    }

    private boolean isDynamicColumnDefaultValue(DataType destDatatype, Object columnDefaultValue) {
        if (columnDefaultValue == null) return false;
        switch (destDatatype) {
            case Instant:
            case LocalDate:
            case LocalDateTime:
                return Character.isAlphabetic(columnDefaultValue.toString().codePointAt(0));
            case Boolean:
            case String:
            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
            case BigDecimal:
            case Json:
                return false;
            default:
                throw new RuntimeException("Unsupported type: " + destDatatype);
        }
    }

    private boolean isSupportedTypePromotion(DataType oldType, DataType newType) {
        return IbfSupportedTypeChanges.isTypeChangeSupported(oldType, newType);
    }

    public boolean doesColumnChangeRequireTableResync(IbfColumnInfo columnInfo) {
        return isDynamicColumnDefaultValue(columnInfo.destColumnType, columnInfo.columnDefaultValue);
    }
}
