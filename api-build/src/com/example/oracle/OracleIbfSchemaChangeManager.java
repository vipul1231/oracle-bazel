package com.example.oracle;

import com.example.core.TableRef;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.schema.*;
import com.example.lambda.Lazy;
import com.example.logger.ExampleLogger;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OracleIbfSchemaChangeManager extends IbfSchemaManager {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private Lazy<Map<TableRef, SchemaChangeResult>> latestIbfTableInfo = new Lazy<>(this::getSchemaChanges);

    private final Collection<OracleIbfTableInfo> oracleIbfTableInfos;

    private final ColumnInfoService columnInfoService;

    public OracleIbfSchemaChangeManager(
            ColumnInfoService columnInfoService,
            IbfPersistentStorage storage,
            ConnectionParameters params,
            Collection<OracleIbfTableInfo> oracleIbfTableInfos) {
        super(storage, params.owner + "-" + params.schema);
        this.columnInfoService = columnInfoService;
        this.oracleIbfTableInfos = oracleIbfTableInfos;
    }

    public SchemaChangeResult getSchemaChange(TableRef table) {
        SchemaChangeResult result = latestIbfTableInfo.get().get(table);

        if (result == null) {
            result = new SchemaChangeResult(SchemaChangeType.NO_CHANGE, ImmutableMap.of(), false);
        }

        return result;
    }

    @Override
    public boolean doesColumnChangeRequireTableResync(IbfColumnInfo columnInfo) {
        OracleIbfColumnInfo oracleIbfColumnInfo = (OracleIbfColumnInfo) columnInfo;
        OracleColumnInfo oracleColumnInfo = oracleIbfColumnInfo.getOracleColumnInfo();

        try {
            Optional<String> dataDefaultExpression = columnInfoService.getDataDefault(oracleColumnInfo);

            if (dataDefaultExpression.isPresent()) {
                // Until then we have to resync the table if the newly added column has a data_default value.
                // The implementation will require calling Output.updateSchema from OracleIbfTableSyncer
                // performIncrementalSync method.

                // Is the data default value a literal value?
                Optional<Object> defaultValue = oracleColumnInfo.parseDataDefaultExpression(dataDefaultExpression);

                if (defaultValue.isPresent()) {
                    oracleIbfColumnInfo.setDefaultValue(defaultValue.get());
                    return false;
                }

            } else {
                // The default value is null so no resync required
                return false;
            }
        } catch (Exception ex) {
            LOG.warning("Could not retrieve column default value for " + oracleColumnInfo);
        }

        return true;
    }

    private Map<TableRef, SchemaChangeResult> getSchemaChanges() {
        Set<IbfTableInfo> ibfTableInfos =
                oracleIbfTableInfos.stream().map(i -> i.getIbfTableInfo()).collect(Collectors.toSet());
        return checkAndSaveSchemaChanges(ibfTableInfos);
    }
}
