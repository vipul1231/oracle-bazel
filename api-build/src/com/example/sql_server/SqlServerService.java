package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core.storage.PersistentStorage;
import com.example.core2.Output;
import com.example.crypto.GroupMasterKeyService;
import com.example.db.*;
import com.example.flag.FlagName;
import com.example.oracle.*;
import com.example.oracle.warnings.WarningType;
import com.example.snowflakecritic.SnowflakeSourceCredentials;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.setup.SnowflakeConnectorSetupForm;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableList;
import oracle.security.crypto.core.KeyPair;

import javax.crypto.SecretKey;
import javax.sql.DataSource;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.example.sql_server.SqlServerServiceType.SQL_SERVER;

public class SqlServerService
        extends DbService<DbCredentials, SqlServerState, SqlServerTableInfo, SqlServerSource, SqlServerInformer> {

    private static final String GCP_TRIDENT_NON_REGIONAL_SA = "";
    SqlServerServiceType serviceType = SQL_SERVER;

    private SqlServerTrident trident;
    private boolean isTridentProducerEnabled;

    public SqlServerService() {}

    @Override
    public String id() {
        return serviceType.id();
    }

    @Override
    public String name() {
        return serviceType.fullName();
    }

    @Override
    public String description() {
        return serviceType.description();
    }

    @Override
    public OnboardingSettings onboardingSettings() {
        return new OnboardingSettings(OnboardingCategory.Databases, OnboardingCategory.ProductAnalytics);
    }

    @Override
    public OracleState initialState() {
        return null;
    }

//    @Override
    public int version() {
        return 2;
    }

//    @Override
    public RenamingRules renamingRules() {
//        return DbRenamingRules();
        return null;
    }

    @Override
    protected SqlServerSource source(DbCredentials dbCredentials, com.example.core2.ConnectionParameters params) {
        return new SqlServerSource(dbCredentials, params);
    }

    @Override
    public SqlServerInformer informer(SqlServerSource source, StandardConfig standardConfig) {
        return new SqlServerInformer(source, standardConfig);
    }

    @Override
    public DbImporter<TableRef> importer(
            SqlServerSource source, SqlServerState state, SqlServerInformer informer, Output<SqlServerState> out) {
        return new SqlServerImporter(source, state, informer, out);
    }

    @Override
    public DbIncrementalUpdater incrementalUpdater(
            SqlServerSource source, SqlServerState state, SqlServerInformer informer, Output<SqlServerState> out) {
        return new SqlServerIncrementalUpdater(source, state, informer, out);
    }

    SqlServerTrident createTrident(
            SqlServerSource source, SqlServerState state, SqlServerInformer informer, Output<SqlServerState> out) {
        GroupMasterKeyService encryptionService = GroupMasterKeyServiceFactory.getDefaultKMSService();
        PersistentStorage persistentStorage =
                Gcs.create(
                        state.getTridentStorageId(),
                        Gcs.GCS_PROD_PROJECT_ID,
                        Gcs.GCS_PROD_BUCKET,
                        SystemCredentialServiceFactory.getDefault(),
                        GCP_TRIDENT_NON_REGIONAL_SA);

//        String encryptionContext = new SystemEncryptionContext(GCP_TRIDENT_NON_REGIONAL_SA).toJsonString();

        if (state.getEncryptedKey() == null) {
//            DataKey datakey =
//                    encryptionService.newDataKey(
//                            encryptionService.getCustomerMasterKey(source.params.owner), encryptionContext);
//            state.setEncryptedKey(datakey.encryptedKey);
        }

        SecretKey secretKey = null;
//        SecretKey secretKey =
//                encryptionService.decryptDataKey(
//                        encryptionService.getCustomerMasterKey(source.params.owner),
//                        state.getEncryptedKey(),
//                        encryptionContext);

//        return new SqlServerTrident(source, state, informer, persistentStorage, secretKey, out);
        return new SqlServerTrident(source, state, informer, null, null, null);
    }


    @Override
    protected void prepareTrident(
            SqlServerInformer informer, Output<SqlServerState> out, SqlServerState state, SqlServerSource source) {
        // assume the flag is enabled
        if (FlagName.SqlServerTrident.check()) {
            trident = createTrident(source, state, informer, out);

//             delete now so we can see if there are still more items in the queue that hasn't been consumed yet
            trident.delete(state.getReadCursors());

//             clear read cursors, they'll get filled again if we consume more items from Trident during the sync
            state.clearReadCursors();

//             Producer thread is enabled if there are any tables to import
            if (trident.isProducerEnabled()) {
                isTridentProducerEnabled = true;
                trident.startProducer();
            } else if (trident.hasMore()) {
                LOG.info("Consuming any remaining items in Trident storage from previous sync");
                trident.read();
            }
        }
    }

    @Override
    public void prepareSync(
            SqlServerSource source,
            SqlServerState state,
            SqlServerInformer informer,
            DbImporter<TableRef> importer,
            Output<SqlServerState> out,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables) {
        super.prepareSync(source, state, informer, importer, out, includedTables, excludedTables);

        prepareSqlSync(source, state, informer, out, includedTables, excludedTables);
    }

    void prepareSqlSync(
            SqlServerSource source,
            SqlServerState state,
            SqlServerInformer informer,
            Output<SqlServerState> out,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables) {

        // Initialize TableState for new tables
        initNewlyIncludedTablesInState(state, includedTables);
        // Remove any tables excluded from the StandardConfig so they'll be re-imported when included again
        removeExcludedTablesFromState(state, excludedTables);
    }

    @Override
    protected void doSync(
            SqlServerInformer informer,
            DbImporter<TableRef> importer,
            List<TableRef> tablesToImport,
            Runnable incrementalUpdate) {
        super.doSync(informer, importer, tablesToImport, isTridentProducerEnabled ? trident::read : incrementalUpdate);
    }

    @Override
    protected void shutdownTrident() {
        if (FlagName.SqlServerTrident.check()) {
            try {
//                if (trident.isProducerEnabled()) {
//                    trident.stopProducer();
//                }
            } finally {
                LOG.info("Closing trident storage");
//                trident.closeStorage();
            }
        }
    }

    void removeExcludedTablesFromState(SqlServerState state, Collection<TableRef> excludedTables) {
        excludedTables.stream().filter(state.tables::containsKey).forEach(state.tables::remove);
    }

    void initNewlyIncludedTablesInState(SqlServerState state, Collection<TableRef> includedTables) {
        includedTables
                .stream()
                .filter(t -> !state.tables.containsKey(t))
                .forEach(t -> state.initTableState(t, ExampleClock.Instant.now()));
    }

    @Override
    public DbServiceType serviceType() {
        return serviceType;
    }

    @Override
    public Path largeIcon() {
        return Paths.get("/integrations/sql_server/resources/sql_server.svg");
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/databases/sql-server");
    }

//    @Override
//    public SqlServerState initialState() {
//        return new SqlServerState();
//    }

    @Override
    public Class<DbCredentials> credentialsClass() {
        return DbCredentials.class;
    }

    @Override
    public boolean supportsCreationViaApi() {
        return true;
    }

    @Override
    public DbCredentials prepareApiCredentials(DbCredentials creds, String schema, WizardContext wizardContext) {
//        creds.publicKey = getSshKey(wizardContext.groupId).getPublicKey();
//        return creds;
        return null;
    }

    @Override
    public LinkedHashMap<String, Object> connectionConfig(DbCredentials creds, OracleState oracleState, String owner, String integration) {
        return null;
    }

//    @Override
//    public void update(DbCredentials credentials, OracleState state, com.example.oracle.ConnectionParameters params, Output<OracleState> output, StandardConfig standardConfig, Set<String> featureFlags) throws CloseConnectionException {
//
//    }

    /** This is a temporary workaround until Pipeline#clearTasks no longer needs a {@link DataUpdater} */
//    @Override
    public DataUpdater<SqlServerState> updater(DbCredentials creds, SqlServerState state, ConnectionParameters params) {
//        return new SqlServerUpdater(state);
        return null;
    }

    @Override
    protected boolean shouldSoftDeleteAfterImport() {
        return true;
    }

    @Override
    public SqlServerSource source(DbCredentials credentials, com.example.core2.ConnectionParameters params, DataSource dataSource) {
        return new SqlServerSource(credentials, params);
    }

    @Override
    protected List<String> tableExcludedReasons(SnowflakeSourceCredentials snowflakeSourceCredentials, SnowflakeTableInfo tableInfo) {
        return null;
    }

    @Override
    public SnowflakeConnectorSetupForm setupForm(WizardContext wizardContext) {
        return null;
    }

    //    @Override
//    public SetupForm<DbCredentials> setupForm(WizardContext wizardContext) {
        KeyPair keyPair = null;
//        return (SetupForm<DbCredentials>) new SqlServerSourceSetupForm(
//                (SqlServerServiceType) serviceType(),
//                null,
//                new ConnectionParameters(wizardContext.groupId, wizardContext.schema, null));
//        return null;
//    }

    /*
    @Override
    public void runSpeedTest(WizardContext wizardContext, DbCredentials credentials, Optional<Long> dataSize) {
        ConnectorSpeedTest test =
                new SqlServerSpeedTest(
                        credentials, new ConnectionParameters(wizardContext.groupId, wizardContext.schema, null));
        test.runTestAndSaveAvgRecord(wizardContext, serviceType.id(), dataSize);
    }
    */


//    @Override
    public double estimateInitialSyncProgress(SqlServerState state, StandardConfig config) {
//        Set<TableRef> includedTables =
//                config.includedTables().stream().map(t -> new TableRef(t.schema.get(), t.table)).collect(toSet());
        Set<TableRef> includedTables =new HashSet<>();
        long totalTables = includedTables.size();
        if (totalTables == 0) {
            return -1;
        } else {
            long tablesFinished = includedTables.stream().filter(state::isImportFinished).count();
            double progress = (double) tablesFinished / (double) totalTables;
            return progress;
        }
    }

//    @Override
    public Iterable<WarningType> warningTypes() {
//        return ImmutableList.of(ResyncTableWarning.TYPE, CdcNewColumnWarning.TYPE);
        return null;
    }

    @Override
    public boolean startPaused() {
        return true;
    }

    @Override
    public String schemaRule(Optional<String> schemaNameFromRecord, com.example.oracle.ConnectionParameters params) {
        return null;
    }

    @Override
    public boolean supportsTableResync() {
        return true;
    }

    @Override
    public OracleState clearTableState(OracleState state, TableRef table) {
        return null;
    }

    @Override
    public boolean canDefineSchema() {
        return false;
    }

    @Override
    public Map<String, ColumnConfig> columnConfig(DbCredentials creds, com.example.oracle.ConnectionParameters params, String schema, String table) {
        return null;
    }

    @Override
    public Optional<List<Object>> pricingDimensions(DbCredentials creds, OracleState state, com.example.oracle.ConnectionParameters params) {
        return Optional.empty();
    }

    @Override
    public boolean hidden() {
        return false;
    }

    @Override
    public boolean beta() {
        return false;
    }

    @Override
    public Map<String, String> customMicrometerLabels(DbCredentials credentials, com.example.oracle.ConnectionParameters params) {
        return null;
    }

    @Override
    public StandardConfig standardConfig(DbCredentials creds, com.example.oracle.ConnectionParameters params, OracleState state) {
        return null;
    }

    //    @Override
    public SqlServerState clearTableState(SqlServerState state, TableRef table) {
        SqlServerState.TableState removedState = state.tables.remove(table);
        if (removedState == null) LOG.warning(table + " was not found in state");
        return state;
    }

//    @Override
    public String schemaRule(Optional<String> schemaNameFromRecord, com.example.core2.ConnectionParameters params) {
        return schemaNameFromRecord
                .map(schema -> schema.replaceFirst(params.schema + "_", ""))
                .orElseThrow(() -> new RuntimeException("Schema name not present in record"));
    }

    @Override
    protected List<String> tableExcludedReasons(SqlServerTableInfo tableInfo) {
        // exclude tables that only contain columns of unsupported type
        return tableInfo.sourceColumnInfo().isEmpty()
                ? ImmutableList.of("Table contains no accessible columns or supported column types")
                : tableInfo.excludeReasons.stream().map(r -> r.message).collect(Collectors.toList());
    }

//    @Override
    public StandardConfig standardConfig(DbCredentials creds, com.example.core2.ConnectionParameters params, SqlServerState state) {
        return sqlStandardConfig(creds, params, state);
    }

//    @Override
    public Optional<List<Object>> pricingDimensions(
            DbCredentials creds, SqlServerState state, com.example.core2.ConnectionParameters params) {
        Map<String, String> map = new HashMap<>();
        map.put("Host", creds.host);
        map.put("Port", creds.port != null ? creds.port.toString() : "");
        creds.database.ifPresent(database -> map.put("Database", database));
        return Optional.of(ImmutableList.of(map));
    }
}
