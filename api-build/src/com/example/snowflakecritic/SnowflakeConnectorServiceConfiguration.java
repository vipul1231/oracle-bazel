package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.ConnectionParameters;
import com.example.core2.Output;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.snowflakecritic.ibf.SnowflakeIbfAdapter;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Sort of like a DI context/configuration class
 */
public class SnowflakeConnectorServiceConfiguration {
    private static Map<Class<?>, Function<SnowflakeConnectorServiceConfiguration, ?>> overrides = new HashMap<>();

    public static void override(Class<?> typeOf, Function<SnowflakeConnectorServiceConfiguration, ?> override) {
        overrides.put(typeOf, override);
    }

    public static void clearOverrides() {
        overrides.clear();
    }

    private SnowflakeSourceCredentials credentials;
    private StandardConfig standardConfig;
    private SnowflakeConnectorState connectorState;
    private Output<SnowflakeConnectorState> output;
    private ConnectionParameters connectionParameters;

    private SingletonBeanFactory<SnowflakeSource> sourceBean =
            new SingletonBeanFactory<>(
                    () -> new SnowflakeSourceWithSession(getCredentials(), getConnectionParameters()));

    private SingletonBeanFactory<SnowflakeInformer> informerBean =
            new SingletonBeanFactory<>(() -> new SnowflakeInformer(getSnowflakeSource(), getStandardConfig()));

    private SingletonBeanFactory<SnowflakeImporter> importerBean =
            new SingletonBeanFactory<>(() -> new SnowflakeImporter(this));

    private SingletonBeanFactory<SnowflakeIncrementalUpdater> incrementalUpdaterBean =
            new SingletonBeanFactory<>(() -> new SnowflakeIncrementalUpdater(this));

    private SingletonBeanFactory<SnowflakeSystemInfo> snowflakeSystemInfoBean =
            new SingletonBeanFactory<>(() -> new SnowflakeSystemInfo(getSnowflakeSource()));

//    private SingletonBeanFactory<SnowflakeTableValidator> snowflakeTableValidatorBean =
//            new SingletonBeanFactory<>(this::createSnowflakeTableValidator);

    private final SingletonBeanFactory<IbfPersistentStorage> ibfPersistentStorage =
            new SingletonBeanFactory<>(this::defaultIbfPersistentStorage);

    private final SingletonBeanFactory<SnowflakeIbfSchemaManager> ibfSchemaManager =
            new SingletonBeanFactory<>(this::defaultIbfSchemaManager);


    public SnowflakeConnectorServiceConfiguration() {
        // Apply overrides
        applyOverride(SnowflakeSource.class, sourceBean);
        applyOverride(SnowflakeInformer.class, informerBean);
        applyOverride(SnowflakeImporter.class, importerBean);
        applyOverride(SnowflakeIncrementalUpdater.class, incrementalUpdaterBean);
        applyOverride(SnowflakeSystemInfo.class, snowflakeSystemInfoBean);
        applyOverride(IbfPersistentStorage.class, ibfPersistentStorage);
    }

    private <T> void applyOverride(Class<T> klass, SingletonBeanFactory<T> beanFactory) {
        if (overrides.containsKey(klass)) {
            Function<SnowflakeConnectorServiceConfiguration, T> override =
                    (Function<SnowflakeConnectorServiceConfiguration, T>) overrides.get(klass);
            beanFactory.override(() -> override.apply(this));
        }
    }

    public SnowflakeSourceCredentials getCredentials() {
        return Objects.requireNonNull(credentials, "Call setCredentials(...) first");
    }

    public SnowflakeConnectorServiceConfiguration setCredentials(SnowflakeSourceCredentials credentials) {
        this.credentials = Objects.requireNonNull(credentials);
        return this;
    }

    public SnowflakeSource getSnowflakeSource() {
        return sourceBean.get();
    }

    public ConnectionParameters getConnectionParameters() {
        return Objects.requireNonNull(connectionParameters, "Call setConnectionParameters(...) first");
    }

    public SnowflakeConnectorServiceConfiguration setSnowflakeSource(SnowflakeSource source) {
        sourceBean.set(source);
        return this;
    }

    public SnowflakeConnectorServiceConfiguration setStandardConfig(StandardConfig standardConfig) {
        this.standardConfig = standardConfig;
        return this;
    }

    public SnowflakeConnectorServiceConfiguration setConnectionParameters(ConnectionParameters connectionParameters) {
        this.connectionParameters = connectionParameters;
        return this;
    }

    public StandardConfig getStandardConfig() {
        return Objects.requireNonNull(standardConfig, "Call setStandardConfig(...) first");
    }

    public SnowflakeInformer getSnowflakeInformer() {
        return informerBean.get();
    }

    public SnowflakeConnectorServiceConfiguration setSnowflakeInformer(SnowflakeInformer informer) {
        informerBean.set(informer);
        return this;
    }

    public SnowflakeImporter getImporter() {
        return importerBean.get();
    }

    public SnowflakeConnectorState getConnectorState() {
        return Objects.requireNonNull(connectorState, "Call setConnectorState first.");
    }

    public SnowflakeConnectorServiceConfiguration setConnectorState(SnowflakeConnectorState connectorState) {
        this.connectorState = connectorState;
        return this;
    }

    public Output<SnowflakeConnectorState> getOutput() {
        return output;
    }

    public SnowflakeConnectorServiceConfiguration setOutput(Output<SnowflakeConnectorState> output) {
        this.output = output;
        return this;
    }

    public SnowflakeIncrementalUpdater getSnowflakeIncrementalUpdater() {
        return incrementalUpdaterBean.get();
    }

    public SnowflakeTableImporter newSnowflakeTableImporter(TableRef tableRef) {
        if (SnowflakeSourceCredentials.UpdateMethod.IBF == getCredentials().updateMethod) {
            return new SnowflakeSimpleTableImporter(this, tableRef);
        }

        return new SnowflakeTimeTravelTableImporter(this, tableRef);
    }

    private Map<TableRef, SnowflakeIbfAdapter> ibfAdapters = new HashMap<>();

    public SnowflakeIbfAdapter getIbfAdapter(TableRef tableRef) {
        return ibfAdapters.computeIfAbsent(tableRef, this::newSnowflakeIbfAdapter);
    }

    private SnowflakeIbfAdapter newSnowflakeIbfAdapter(TableRef tableRef) {
        return new SnowflakeIbfAdapter(
                getSnowflakeSource().getDataSource(), getSnowflakeInformer().tableInfo(tableRef));
    }

    private static Map<TableRef, IbfCheckpointManager> ibfCheckpointManagers = new HashMap<>();

    public IbfCheckpointManager<SnowflakeIbfAdapter> getIbfCheckpointManager(TableRef tableRef, IbfCheckpointManager checkpointManager) {
        if (checkpointManager != null) {
            ibfCheckpointManagers.put(tableRef, checkpointManager);
            return checkpointManager;
        }

        return getIbfCheckpointManager(tableRef);
    }

    public IbfCheckpointManager<SnowflakeIbfAdapter> getIbfCheckpointManager(TableRef tableRef) {
        return ibfCheckpointManagers.computeIfAbsent(tableRef, this::newIbfCheckpointManager);
    }

    private IbfCheckpointManager<SnowflakeIbfAdapter> newIbfCheckpointManager(TableRef tableRef) {
        return new IbfCheckpointManager<>(
                getIbfAdapter(tableRef),
                getIbfPersistentStorage(),
                getConnectorState().getTableState(tableRef).getIbfObjectId());
    }

    public SnowflakeTableUpdater newSnowflakeTableUpdater(TableRef tableRef) {
        if (SnowflakeSourceCredentials.UpdateMethod.IBF == getCredentials().updateMethod) {
            return new SnowflakeIbfTableUpdater(
                    this, getIbfSchemaManager(), tableRef, getIbfCheckpointManager(tableRef, null));
        }

        return new SnowflakeTimeTravelTableUpdater(this, tableRef);
    }

    public SnowflakeSystemInfo getSnowflakeSystemInfo() {
        return snowflakeSystemInfoBean.get();
    }

    public SnowflakeTableValidator getSnowflakeTableValidator() {
        return null;
        //return snowflakeTableValidatorBean.get();
    }

//    private SnowflakeTableValidator createSnowflakeTableValidator() {
//        if (SnowflakeSourceCredentials.UpdateMethod.IBF == getCredentials().updateMethod) {
//            return new SnowflakeIbfTableValidator();
//        }
//
//        return new SnowflakeTimeTravelTableValidator();
//    }

    public IbfPersistentStorage getIbfPersistentStorage() {
        return ibfPersistentStorage.get();
    }

    private IbfPersistentStorage defaultIbfPersistentStorage() {
        return IbfPersistentStorage.newBuilder(getEncryptionKey()).build();
    }

    private SnowflakeIbfSchemaManager defaultIbfSchemaManager() {
        ConnectionParameters params = getConnectionParameters();
        String ibfObjectId = params.owner + "-" + params.schema;

        SnowflakeInformer informer = getSnowflakeInformer();

        Set<SnowflakeTableInfo> tableInfos =
                informer.includedTables()
                        .stream()
                        .map(tableRef -> informer.tableInfo(tableRef))
                        .collect(Collectors.toSet());

        return new SnowflakeIbfSchemaManager(getIbfPersistentStorage(), ibfObjectId, tableInfos);
    }

    public SnowflakeIbfSchemaManager getIbfSchemaManager() {
        return ibfSchemaManager.get();
    }

    public SecretKey getEncryptionKey() {
        return null;
//        EncryptionService encryptionService = encryptionServiceSingleton.get();
//
//        SnowflakeConnectorState state = getConnectorState();
//        if (state.encryptedKey == null) {
//            DataKey dataKey =
//                    encryptionService.getDataKey(GroupCredentialType.GROUP_DATA, getConnectionParameters().owner);
//            state.encryptedKey = dataKey.encryptedKey;
//        }
//        return encryptionService.decryptDataKey(
//                GroupCredentialType.GROUP_DATA, getConnectionParameters().owner, state.encryptedKey);
    }



    public Optional<PreTableImportHandler> getPreTableImportHandler(TableRef tableRef) {
        if (SnowflakeSourceCredentials.UpdateMethod.IBF == getCredentials().updateMethod) {
            return Optional.of(new IbfPreTableImportHandler(this, tableRef));
        }

        return Optional.empty();
    }
}
