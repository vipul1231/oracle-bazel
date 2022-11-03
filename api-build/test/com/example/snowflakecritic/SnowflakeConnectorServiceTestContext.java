package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.example.core2.ConnectionParameters;
import com.example.core2.Output;
import com.example.crypto.Encrypt;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.lambda.Lazy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration.clearOverrides;
import static com.example.snowflakecritic.SnowflakeConnectorServiceConfiguration.override;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Encapsulates all the objects that are passed around various methods calls. Simplifies calls to the Service object.
 */
public class SnowflakeConnectorServiceTestContext {

    private SnowflakeConnectorService service;

    private StandardConfig standardConfig;
    private SnowflakeSourceCredentials credentials;
    private ConnectionParameters connectionParameters;
    private Output<SnowflakeConnectorState> output;
    private SingletonBeanFactory<SnowflakeConnectorState> connectorStateBean =
            new SingletonBeanFactory<>(() -> service.initialState());

    private SingletonBeanFactory<SnowflakeSource> sourceBean =
            new SingletonBeanFactory<>(() -> service.source(credentials, connectionParameters));

    private SingletonBeanFactory<SnowflakeInformer> informerBean =
            new SingletonBeanFactory<>(() -> service.informer(getSource(), Objects.requireNonNull(standardConfig)));

    private static final Lazy<Path> STORAGE_DIR =
            new Lazy<>(() -> Files.createTempDirectory("local_disk_ibf_storage_client-"));

    public static class TestSnowflakeInformer extends SnowflakeInformer {

        private Map<TableRef, SnowflakeTableInfo> tableInfos = new HashMap<>();

        public TestSnowflakeInformer(SnowflakeSource snowflakeSource, StandardConfig standardConfig) {
            super(snowflakeSource, standardConfig);
        }

        public TestSnowflakeInformer tableInfos(SnowflakeTableInfo... args) {
            for (SnowflakeTableInfo tableInfo : args) {
                tableInfos.put(tableInfo.sourceTable, tableInfo);
            }
            return this;
        }


        public SnowflakeInformationSchemaDao newInformationSchemaDao() {
            return new SnowflakeInformationSchemaDao(source, standardConfig, excludedColumns()) {

                @Override
                public Map<TableRef, SnowflakeTableInfo> fetchTableInfo() {
                    return tableInfos;
                }
            };
        }
    }

    public SnowflakeConnectorServiceTestContext(SnowflakeConnectorService service) {
        this.service = service;

        //SnowflakeConnectorServiceConfiguration.override(EncryptionService.class, config -> mockEncryptionService());
        SnowflakeConnectorServiceConfiguration.override(
                IbfPersistentStorage.class,
                config ->
                        IbfPersistentStorage.newBuilder(Encrypt.newEphemeralKey())
                                .withLocalDiskStorage(STORAGE_DIR.get())
                                .build());
    }

    public void cleanup() {
        clearOverrides();
    }

    public SnowflakeConnectorService getService() {
        return service;
    }

    public SnowflakeConnectorState getConnectorState() {
        return connectorStateBean.get();
    }

    public SnowflakeConnectorServiceTestContext setConnectorState(SnowflakeConnectorState connectorState) {
        connectorStateBean.set(connectorState);
        return this;
    }

    public SnowflakeConnectorServiceTestContext setStandardConfig(StandardConfig standardConfig) {
        this.standardConfig = standardConfig;
        return this;
    }

    public SnowflakeConnectorServiceTestContext withMockStandardConfig() {
        this.standardConfig = mock(StandardConfig.class);
        when(standardConfig.excludedTables()).thenReturn(new HashMap<>());
        when(standardConfig.syncModes()).thenReturn(new HashMap<>());
        return this;
    }

    public SnowflakeConnectorServiceTestContext setCredentials(SnowflakeSourceCredentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public SnowflakeConnectorServiceTestContext setConnectionParameters(ConnectionParameters connectionParameters) {
        this.connectionParameters = connectionParameters;
        return this;
    }

    public SnowflakeConnectorServiceTestContext setOutput(Output<SnowflakeConnectorState> output) {
        this.output = output;
        return this;
    }

    public SnowflakeConnectorServiceTestContext withMockOutput() {
        return setOutput(new MockOutput2<>(getConnectorState()));
    }

    public SnowflakeConnectorServiceTestContext setSnowflakeInformer(
            Function<SnowflakeConnectorServiceConfiguration, SnowflakeInformer> override) {
        override(SnowflakeInformer.class, override);
        return this;
    }

    public SnowflakeConnectorServiceTestContext setSnowflakeSource(SnowflakeSource source) {
        override(SnowflakeSource.class, config -> source);
        if (source.originalCredentials != null) {
            setCredentials(source.originalCredentials);
        }
        return this;
    }

    public SnowflakeConnectorServiceTestContext setSnowflakeSystemInfo(SnowflakeSystemInfo snowflakeSystemInfo) {
        override(SnowflakeSystemInfo.class, config -> snowflakeSystemInfo);
        return this;
    }

    public SnowflakeSource getSource() {
        return sourceBean.get();
    }

    public SnowflakeInformer getInformer() {
        return informerBean.get();
    }

    public SnowflakeImporter getImporter() {
        return service.importer(getSource(), getConnectorState(), getInformer(), output);
    }

    public SnowflakeIncrementalUpdater getIncrementalUpdater() {
        return service.incrementalUpdater(getSource(), getConnectorState(), getInformer(), output);
    }

    public StandardConfig getStandardConfig() {
        return service.standardConfig(credentials, connectionParameters, getConnectorState());
    }

    public List<String> getTableExcludedReasons(SnowflakeTableInfo tableInfo) {
        return service.tableExcludedReasons(credentials, tableInfo);
    }

    public void runUpdate() {
        service.update(credentials, getConnectorState(), connectionParameters, output, standardConfig, new HashSet<>());
    }

    public SnowflakeConnectorServiceTestContext withTestSnowflakeInformer(SnowflakeTableInfo... args) {
        setSnowflakeInformer(
                config ->
                        new TestSnowflakeInformer(config.getSnowflakeSource(), config.getStandardConfig())
                                .tableInfos(args));
        return this;
    }

    public SnowflakeConnectorServiceTestContext withMockSnowflakeSystemInfo() {
        SnowflakeSystemInfo mock = mock(SnowflakeSystemInfo.class);
        when(mock.getSystemTime()).thenReturn(Instant.now().toString());
        setSnowflakeSystemInfo(mock);
        return this;
    }

    public Output<SnowflakeConnectorState> getOutput() {
        return output;
    }

//    public EncryptionService mockEncryptionService() {
//        EncryptionService mock = mock(EncryptionService.class);
//        when(mock.getDataKey(any(), any())).thenReturn(new DataKey("test".getBytes(), "test".getBytes()));
//        when(mock.decryptDataKey(any(), any(), any()))
//                .thenReturn(new SecretKeySpec(new byte[16], 0, 16, Encrypt.DEFAULT_ALGORITHM));
//        return mock;
//    }
}
