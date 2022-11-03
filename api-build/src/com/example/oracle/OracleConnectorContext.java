package com.example.oracle;

import com.example.core.OracleCredentials;
import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core.storage.PersistentStorage;
import com.example.core2.Output;
import com.example.crypto.GroupMasterKeyService;
import com.example.flag.FeatureFlag;
import com.example.logger.ExampleLogger;
import com.example.oracle.spi.OracleAbstractIncrementalUpdater;
import com.example.oracle.spi.OracleIncrementalUpdaterFactory;
import com.example.snowflakecritic.SingletonBeanFactory;
import oracle.jdbc.pool.OracleDataSource;

import javax.crypto.SecretKey;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** Encapsulates object creation to reduce complexity related to instantiating objects. */
/**
 * A super Builder that encapsulates object creation to reduce complexity related to instantiating objects and allows
 * optional overriding of factory methods.
 *
 * <p>This can be thought of as similar to a context or configuration object in a dependency injection framework.
 */
public class OracleConnectorContext implements OracleIncrementalUpdaterFactory {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private OracleApi oracleApi;
    private OracleService oracleService;
    private ConnectionParameters connectionParameters;
    private OracleResyncHelper oracleResyncHelper = new OracleResyncHelper();
    private OracleMetricsHelper oracleMetricsHelper = new OracleMetricsHelper();
    private OracleState oracleState;
    private StandardConfig standardConfig;

    private MetaDataUtil metaDataUtil;

    private HashIdGenerator hashIdGenerator;

    //    private AbstractHashIdGenerator hashIdGenerator = new HashIdGeneratorImpl();
    private Output<OracleState> output;
    private OracleOutputHelper oracleOutputHelper;
//    private SingletonBeanFactory<GroupMasterKeyService> groupMasterKeyService =
//            new SingletonBeanFactory<>(this::defaultGroupMasterKeyService);
//    private SingletonBeanFactory<PersistentStorage> persistentStorage =
//            new SingletonBeanFactory<>(this::defaultPersistentStorage);
//    private SingletonBeanFactory<SystemCredentialService> systemCredentialService =
//            new SingletonBeanFactory<>(this::createSystemCredentialService);
//    private SingletonBeanFactory<SystemCredentialType> systemCredentialType =
//            new SingletonBeanFactory<>(() -> SystemCredentialType.GCP_TRIDENT_NON_REGIONAL_SA);
//    private SingletonBeanFactory<String> gcsProjectId = new SingletonBeanFactory<>(() -> Gcs.GCS_PROD_PROJECT_ID);
//    private SingletonBeanFactory<String> gcsBucket = new SingletonBeanFactory<>(() -> Gcs.GCS_PROD_BUCKET);
//
//    private List<Consumer<TridentPersistentStorage2.Builder<FlashbackEntry>>> postInitializers = new ArrayList<>();

    private final SingletonBeanFactory<DataSource> dataSource;
    private final SingletonBeanFactory<DbObjectValidator> dbObjectValidatorSingleton;
    private PersistentStorage persistentStorage;

    public DbObjectValidator getDbObjectValidator() {
        return dbObjectValidatorSingleton.get();
    }

    public OracleConnectorContext(SingletonBeanFactory<DbObjectValidator> dbObjectValidatorSingleton) {
        this.dbObjectValidatorSingleton = dbObjectValidatorSingleton;
        dataSource = null;
    }

    public OracleApi getOracleApi() {
        return oracleApi;
    }

    public OracleConnectorContext oracleApi(OracleApi oracleApi) {
        this.oracleApi = oracleApi;
        return this;
    }

    public OracleCredentials getCredentials() {
        return null;
    }


    public OracleService getOracleService() {
        return oracleService;
    }

    public OracleConnectorContext oracleService(OracleService oracleService) {
        this.oracleService = oracleService;
        return this;
    }

    public OracleTableInfoProvider getOracleTableInfoProvider(Map<TableRef, List<OracleColumn>> selected) {
        return new OracleTableInfoContext(
                getOracleTableMetricsProvider(), getOracleState(), getStandardConfig(), selected);
    }

    public ConnectionParameters getConnectionParameters() {
        return connectionParameters;
    }

    public OracleConnectorContext connectionParameters(ConnectionParameters connectionParameters) {
        this.connectionParameters = connectionParameters;
        return this;
    }

    public OracleTableMetricsProvider getOracleTableMetricsProvider() {
        return null;
        //return oracleTableMetricsProvider.get();
    }

    public StandardConfig getStandardConfig() {
        return standardConfig;
    }


    public OracleResyncHelper getOracleResyncHelper() {
        return oracleResyncHelper;
    }

    public OracleMetricsHelper getOracleMetricsHelper() {
        return oracleMetricsHelper;
    }

    public OracleConnectorContext oracleResyncHelper(OracleResyncHelper oracleResyncHelper) {
        this.oracleResyncHelper = oracleResyncHelper;
        return this;
    }

    public HashIdGenerator getHashIdGenerator() {
        return this.hashIdGenerator;
    }

    public OracleState getOracleState() {
        return oracleState;
    }

    public OracleConnectorContext oracleState(OracleState oracleState) {
        this.oracleState = oracleState;
        return this;
    }

    public Output<OracleState> getOutput() {
        return output;
    }

    public OracleConnectorContext output(Output<OracleState> output) {
        this.output = output;
        return this;
    }

    public OracleOutputHelper getOracleOutputHelper() {
        return oracleOutputHelper;
    }

    public OracleConnectorContext oracleOutputHelper(OracleOutputHelper oracleOutputHelper) {
        this.oracleOutputHelper = oracleOutputHelper;
        return this;
    }

    @Override
    public OracleAbstractIncrementalUpdater newIncrementalUpdater(Map<TableRef, List<OracleColumn>> selected) {
        OracleState state = required(getOracleState(), "OracleState");

        if (FeatureFlag.check("OracleTrident")) {
            if (!FeatureFlag.check("OracleFlashback")) {
                throw new RuntimeException("Oracle Flashback flag not enabled. Trident only works with flashback");
            }

//            return new OracleTridentIncrementalUpdater(
//                    state,
//                    getOracleApi(),
//                    getHashIdGenerator(),
//                    getOracleOutputHelper(),
//                    getOracleResyncHelper(),
//                    getOracleMetricsHelper(),
//                    selected,
//                    getConnectionParameters(),
//                    createOracleTrident(getOracleState()));
            return null;
        }

        // Note: it is possible that a connector was previously on the OracleTrident flag but now is no longer.
        // If that is the case then we need to clear out old trident state from the OracleState object.
//        if (null != state.getTridentStorageId()) {
//            LOG.warning(
//                    "Clearing trident state variables from OracleState as this connector is no longer on the OracleTrident feature flag.");
//            state.clearTridentState();
//        }

//        return new OracleIncrementalUpdater(
//                state,
//                getOracleApi(),
//                getHashIdGenerator(),
//                getOracleOutputHelper(),
//                getOracleResyncHelper(),
//                getOracleMetricsHelper(),
//                selected);

        return null;
    }

    public OracleUpdater newOracleUpdater() {
//        return new OracleUpdater(
//                getOracleService(),
//                getOracleApi(),
//                getOracleState(),
//                getConnectionParameters(),
//                null,
//                this);
        return null;
    }

    public OracleOutputHelper createOracleOutputHelper(Output<OracleState> output, StandardConfig standardConfig) {
        output(output);
        oracleOutputHelper =
                new OracleOutputHelper(
                        output, getConnectionParameters().schema, standardConfig, this.getOracleMetricsHelper());
        return oracleOutputHelper;
    }

    public GroupMasterKeyService getGroupMasterKeyService() {
//        return groupMasterKeyService.get();
        return null;
    }

    public OracleConnectorContext groupMasterKeyService(GroupMasterKeyService service) {
//        this.groupMasterKeyService.set(service);
        return this;
    }

    public PersistentStorage getPersistentStorage() {
        return persistentStorage.get();
    }

    public OracleConnectorContext persistentStorage(PersistentStorage persistentStorage) {
//        this.persistentStorage.set(persistentStorage);
        return this;
    }

//    public OracleConnectorContext registerPostInitializer(
//            Consumer<TridentPersistentStorage2.Builder<FlashbackEntry>> postInitializer) {
//        postInitializers.add(postInitializer);
//        return this;
//    }

//    public EncryptionKeyGenerator createEncryptionKeyGenerator() {
//        return new EncryptionKeyGenerator(required(getGroupMasterKeyService(), "GroupMasterKeyService"));
//    }

//    public OracleTrident createOracleTrident(OracleState state) {
//        OracleTrident trident =
//                new OracleTrident(
//                        state,
//                        required(getConnectionParameters(), "ConnectionParameters").owner, // same as groupId
//                        required(getPersistentStorage(), "PersistentStorage"),
//                        createEncryptionKeyGenerator());
//        trident.setPostInitializers(postInitializers);
//        trident.initialize();
//        return trident;
//    }

//    public OracleConnectorContext systemCredentialService(SystemCredentialService systemCredentialService) {
//        this.systemCredentialService.set(systemCredentialService);
//        return this;
//    }
//
//    public SystemCredentialService getSystemCredentialService() {
//        return systemCredentialService.get();
//    }
//
//    private SystemCredentialService createSystemCredentialService() {
//        return SystemCredentialServiceFactory.getDefault();
//    }
//
//    public OracleConnectorContext systemCredentialType(SystemCredentialType type) {
//        systemCredentialType.set(type);
//        return this;
//    }
//
//    public SystemCredentialType getSystemCredentialType() {
//        return systemCredentialType.get();
//    }

//    public OracleConnectorContext gcsProjectId(String value) {
//        gcsProjectId.set(value);
//        return this;
//    }
//
//    public OracleConnectorContext gcsBucket(String value) {
//        gcsBucket.set(value);
//        return this;
//    }

//    public String getGcsProjectId() {
//        return gcsProjectId.get();
//    }
//
//    public String getGcsBucket() {
//        return gcsBucket.get();
//    }

//    private PersistentStorage defaultPersistentStorage() {
//        return Gcs.create(
//                required(getOracleState(), "OracleState. Did you forget to call oracleState()")
//                        .initializeTridentStorageId()
//                        .getTridentStorageId(),
//                getGcsProjectId(),
//                getGcsBucket(),
//                getSystemCredentialService(),
//                getSystemCredentialType());
//    }

//    private GroupMasterKeyService defaultGroupMasterKeyService() {
//        return GroupMasterKeyServiceFactory.getDefaultKMSService();
//    }

    public static <T> T required(T value, String name) {
        if (null == value) {
            throw new IllegalStateException("Missing required value " + name);
        }

        return value;
    }

    public final DataSource getDataSource() {
        return dataSource.get();
    }

    private DataSource defaultDataSource() throws SQLException {
        return new OracleDataSource();
    }

    public MetaDataUtil getMetaDataUtil() {
      return new MetaDataUtil();
    }

    public SyncAdapter getOracleSyncAdapter() {
        return new SyncAdapter();
    }

    public String getStorageId() {
        return oracleState.getStorageId();
    }

    public OracleSyncAdapter oracleSyncAdapter(SyncAdapter syncAdapter) {
        return new OracleSyncAdapter(syncAdapter);
    }

    public SecretKey getEncryptionKey(OracleState state, String groupId) {
        return null;
    }

    /**
     * @TODO: Need to come up wiyth logic
     * @return
     */
    public TimestampConverter getTimestampConverter() {
       return null;
    }
}