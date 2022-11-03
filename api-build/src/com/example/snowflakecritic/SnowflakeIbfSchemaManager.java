package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.schema.IbfSchemaManager;
import com.example.ibf.schema.IbfTableInfo;
import com.example.ibf.schema.SchemaChangeResult;
import com.example.ibf.schema.SchemaChangeType;
import com.example.lambda.Lazy;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SnowflakeIbfSchemaManager extends IbfSchemaManager {
    private Lazy<Map<TableRef, SchemaChangeResult>> latestIbfTableInfo = new Lazy<>(this::getSchemaChanges);
    private final Set<IbfTableInfo> ibfTableInfos;
    private final Map<TableRef, SnowflakeIbfTableInfo> tableInfoMap;

    public SnowflakeIbfSchemaManager(
            IbfPersistentStorage storage, String objectId, Set<SnowflakeTableInfo> snowflakeTableInfos) {
        super(storage, objectId);
        List<SnowflakeIbfTableInfo> snowflakeIbfTableInfos =
                snowflakeTableInfos
                        .stream()
                        .map(tableInfo -> new SnowflakeIbfTableInfo(tableInfo))
                        .collect(Collectors.toList());

        this.ibfTableInfos =
                snowflakeIbfTableInfos.stream().map(s -> s.getIbfTableInfo()).collect(Collectors.toSet());

        this.tableInfoMap =
                snowflakeIbfTableInfos
                        .stream()
                        .collect(Collectors.toMap(tableInfo -> tableInfo.getTableRef(), tableInfo -> tableInfo));
    }

    public SchemaChangeResult getSchemaChange(TableRef table) {
        SchemaChangeResult result = latestIbfTableInfo.get().get(table);

        if (result == null) {
            result = new SchemaChangeResult(SchemaChangeType.NO_CHANGE, ImmutableMap.of(), false);
        }

        return result;
    }

    private Map<TableRef, SchemaChangeResult> getSchemaChanges() {
        return checkAndSaveSchemaChanges(ibfTableInfos);
    }

    public SnowflakeIbfTableInfo getSnowflakeIbfTableInfo(TableRef tableRef) {
        return Objects.requireNonNull(tableInfoMap.get(tableRef), "IbfTableInfo not found for " + tableRef);
    }
}
