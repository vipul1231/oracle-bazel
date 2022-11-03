package com.example.oracle;

import com.example.core.PeriodicCheckpointer;
import com.example.core.TableRef;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.oracle.util.ConnectionManagerWithRetry;
import com.example.snowflakecritic.SingletonBeanFactory;
import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Sort of like a DI-context (such as in Spring). */
public class OracleIbfContext implements OracleIncrementalUpdaterFactory {
    private final Duration CHECKPOINT_PERIOD = Duration.ofMinutes(15);

    private static Map<Class<?>, Supplier<?>> overrides = new HashMap<>();

    public static void override(Class<?> typeOf, Supplier<?> override) {
        overrides.put(typeOf, override);
    }

    private OracleConnectorContext parentContext;
    private Map<TableRef, OracleIbfTableInfo> ibfTableInfoMap = new HashMap<>();
    private OracleTableInfoProvider oracleTableInfoProvider;
    private final SingletonBeanFactory<IbfPersistentStorage> ibfPersistentStorage =
            new SingletonBeanFactory<>(this::defaultIbfPersistentStorage);

    private Function<OracleTableInfo, OracleIBFQueryBuilder> oracleIBFQueryBuilderFactory =
            this::newOracleIBFQueryBuilder;

    private final SingletonBeanFactory<ConnectionManagerWithRetry> connectionManagerSingleton =
            new SingletonBeanFactory<>(this::newConnectionManager);

    private final SingletonBeanFactory<ColumnInfoService> columnInfoServiceSingleton =
            new SingletonBeanFactory<>(this::newColumnInfoService);

    public OracleIbfContext(OracleConnectorContext parentContext) {
        this.parentContext = parentContext;

        if (overrides.containsKey(IbfPersistentStorage.class)) {
            ibfPersistentStorage.override(
                    () -> (IbfPersistentStorage) overrides.get(IbfPersistentStorage.class).get());
        }

        if (overrides.containsKey(ColumnInfoService.class)) {
            columnInfoServiceSingleton.override(() -> (ColumnInfoService) overrides.get(ColumnInfoService.class).get());
        }
    }

    private OracleIbfAdapter getOracleIbfAdapter(OracleIbfTableInfo ibfTableInfo) {
        return newOracleIbfAdapter(ibfTableInfo);
    }

    OracleIbfAdapter getOracleIbfAdapter(OracleTableInfo tableInfo) {
        return getOracleIbfAdapter(getOracleIbfTableInfo(tableInfo.getTableRef()));
    }

    public OracleIBFQueryBuilder getOracleIBFQueryBuilder(OracleTableInfo tableInfo) {
        return oracleIBFQueryBuilderFactory.apply(tableInfo);
    }

    private OracleIBFQueryBuilder newOracleIBFQueryBuilder(OracleTableInfo tableInfo) {
        return new OracleIBFQueryBuilder(getOracleIbfTableInfo(tableInfo.getTableRef()));
    }

    @VisibleForTesting
    OracleIbfTableSyncer newOracleIbfTableSyncer(
            OracleIbfSchemaChangeManager oracleIbfSchemaChangeManager,
            OracleTableState tableState,
            OracleIbfTableInfo ibfTableInfo) {
        OracleApi oracleApi = parentContext.getOracleApi();
        oracleApi.setOutputHelper(parentContext.getOracleOutputHelper());
        return new OracleIbfTableSyncer(
                oracleApi,
                tableState,
                ibfTableInfo,
                oracleIbfSchemaChangeManager,
                getCheckpointManager(ibfTableInfo, tableState.getIbfStorageId()),
                parentContext.getDataSource(),
                parentContext.getOracleOutputHelper(),
                parentContext.getDbObjectValidator());
    }

    private IbfPersistentStorage defaultIbfPersistentStorage() {
        return null;
        //return IbfPersistentStorage.newBuilder(parentContext.getEncryptionKey()).build();
    }

    public IbfPersistentStorage getIbfPersistentStorage() {
        return ibfPersistentStorage.get();
    }

    public OracleIbfContext setSelectedTables(Map<TableRef, List<OracleColumn>> selectedTables) {
        this.oracleTableInfoProvider = parentContext.getOracleTableInfoProvider(Objects.requireNonNull(selectedTables));
        return this;
    }

    @Override
    public OracleAbstractIncrementalUpdater newIncrementalUpdater(Map<TableRef, List<OracleColumn>> selectedTables) {
        setSelectedTables(selectedTables);
        return new OracleIncrementalUpdaterIbf(
                parentContext.getOracleState(),
                parentContext.getOracleOutputHelper(),
                parentContext.getOracleResyncHelper(),
                parentContext.getOracleMetricsHelper(),
                selectedTables,
                getIbfTableWorkers(getOracleTableInfoProvider()));
    }

    private Collection<IbfTableWorker> getIbfTableWorkers(OracleTableInfoProvider oracleTableInfoProvider) {

        Map<TableRef, OracleIbfTableInfo> ibfTableInfos =
                oracleTableInfoProvider
                        .getAllOracleTableInfo()
                        .stream()
                        .collect(Collectors.toMap(i -> i.getTableRef(), i -> newOracleIbfTableInfo(i)));

        OracleIbfSchemaChangeManager schemaChangeManager =
                newOracleIbfSchemaChangeManager(ibfTableInfos.values());

        return oracleTableInfoProvider
                .getAllOracleTableInfo()
                .stream()
                .map(
                        i ->
                                newOracleIbfTableSyncer(
                                        schemaChangeManager,
                                        oracleTableInfoProvider.getOracleTableState(i.getTableRef()),
                                        ibfTableInfos.get(i.getTableRef())))
                .collect(Collectors.toList());
    }

    private OracleIbfAdapter newOracleIbfAdapter(OracleIbfTableInfo ibfTableInfo) {
        return new OracleIbfAdapter(
                parentContext.getDataSource(),
                ibfTableInfo.getOracleTableInfo(),
                ibfTableInfo,
                getOracleIBFQueryBuilder(ibfTableInfo.getOracleTableInfo()),
                new PeriodicCheckpointer(parentContext.getOutput(), parentContext.getOracleState(), CHECKPOINT_PERIOD));
    }

    @VisibleForTesting
    IbfCheckpointManager<OracleIbfAdapter> getCheckpointManager(
            OracleIbfTableInfo ibfTableInfo, String ibfStorageId) {
        return new IbfCheckpointManager<>(
                getOracleIbfAdapter(ibfTableInfo), getIbfPersistentStorage(), ibfStorageId);
    }

    private OracleIbfSchemaChangeManager newOracleIbfSchemaChangeManager(
            Collection<OracleIbfTableInfo> oracleIbfTableInfos) {
        return new OracleIbfSchemaChangeManager(
                getColumnInfoService(),
                Objects.requireNonNull(getIbfPersistentStorage()),
                parentContext.getConnectionParameters(),
                oracleIbfTableInfos);
    }

    public OracleIbfTableInfo getOracleIbfTableInfo(TableRef tableRef) {
        return ibfTableInfoMap.computeIfAbsent(
                tableRef,
                tableRef1 -> newOracleIbfTableInfo(getOracleTableInfoProvider().getOracleTableInfo(tableRef)));
    }

    private OracleIbfTableInfo newOracleIbfTableInfo(OracleTableInfo oracleTableInfo) {
        return new OracleIbfTableInfo(oracleTableInfo);
    }

    public OracleTableInfoProvider getOracleTableInfoProvider() {
        return Objects.requireNonNull(oracleTableInfoProvider, "Call setSelectedTables(...) first");
    }

    private ConnectionManagerWithRetry newConnectionManager() {
        return new ConnectionManagerWithRetry(parentContext.getDataSource(), parentContext.getCredentials().database);
    }

    private ColumnInfoService newColumnInfoService() {
        return null;
        //return new ColumnInfoServiceImpl(connectionManagerSingleton.get());
    }

    public ColumnInfoService getColumnInfoService() {
        return columnInfoServiceSingleton.get();
    }
}
