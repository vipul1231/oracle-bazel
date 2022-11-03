package com.example.oracle;

import com.example.core.*;
import com.example.core2.Output;
import com.example.flag.FeatureFlag;
import com.example.logger.ExampleLogger;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.example.oracle.Constants.sampleSchemas;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:49 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleService implements Service<DbCredentials, OracleState> {
    public OracleServiceType serviceType;
    public final boolean hidden;
    public final boolean beta;
    public static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public OracleService(OracleServiceType serviceType, boolean hidden) {
        this(serviceType, hidden, false);
    }

    public OracleService(OracleServiceType serviceType, boolean hidden, boolean beta) {
        this.serviceType = serviceType;
        this.hidden = hidden;
        this.beta = beta;
    }

    @Override
    public String id() {
        return serviceType.id();
    }

    @Override
    public String name() {
        return serviceType.fullName();
    }

    @Override
    public ServiceType type() {
        return ServiceType.Database;
    }

    @Override
    public String description() {
        return serviceType.description();
    }

    @Override
    public Path largeIcon() {
        return Paths.get("/integrations/oracle/resources/oracle.svg");
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/databases/oracle");
    }

    @Override
    public OnboardingSettings onboardingSettings() {
        return new OnboardingSettings(OnboardingCategory.Databases, OnboardingCategory.ProductAnalytics);
    }

    @Override
    public OracleState initialState() {
        return new OracleState();
    }

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
        return creds;
    }

//    @Override
//    public RenamingRules renamingRules() {
//        return DbRenamingRules();
//    }

    @Override
    public LinkedHashMap<String, Object> connectionConfig(
            DbCredentials creds, OracleState oracleState, String owner, String integration) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("Host", creds.host);
        map.put("Port", creds.port != null ? creds.port.toString() : "");
        creds.database.ifPresent(database -> map.put("Database", database));
        map.put("User", creds.user);
        map.put("Password", "********");

        if (creds.tunnelUser.isPresent()) {
            map.put("Tunnel Host", creds.tunnelHost.orElse(""));
//            map.put("Tunnel Port", creds.tunnelPort.isPresent() ? creds.tunnelPort.get().toString() : "");
            map.put("Tunnel User", creds.tunnelUser.orElse(""));
//            map.put("Public Key", getSshKey(owner).getPublicKey());
//            map.put("Require TLS through Tunnel", String.valueOf(creds.alwaysEncrypted));
        }
        return map;
    }

//    @Override
    public void update(
            DbCredentials credentials,
            OracleState state,
            ConnectionParameters params,
            Output<OracleState> output,
            StandardConfig standardConfig,
            Set<String> featureFlags) throws CloseConnectionException {
        // Automatically call close method on updater
        try (OracleUpdater updater = (OracleUpdater) updater(credentials, state, params)) {
            updater.update2(output, standardConfig);
        }
    }

    private Object updater(DbCredentials credentials, OracleState state, ConnectionParameters params) {
        return null;
    }

    //
    @Override
    public boolean targetCore2Directly(Set<String> featureFlags) {
        return true;
    }
//
//    @Override
//    public DataUpdater<OracleState> updater(DbCredentials credentials, OracleState state, ConnectionParameters params) {
//        return new OracleConnectorContext()
//                .oracleService(this)
//                .oracleApi(productionApi(credentials, params))
//                .oracleState(state)
//                .connectionParameters(params)
//                .newOracleUpdater();
//    }

//    @Override
//    public void runSpeedTest(WizardContext wizardContext, DbCredentials credentials, Optional<Long> dataSize) {
//        ConnectorSpeedTest test =
//                new OracleSpeedTest(
//                        new ConnectionParameters(wizardContext.groupId, wizardContext.schema, null), credentials);
//        test.runTestAndSaveAvgRecord(wizardContext, serviceType.id(), dataSize);
//    }

    public OracleApi productionApi(DbCredentials credentials, ConnectionParameters connectionParams) {
//        LoggableDataSource loggableSource =
//                Retry.act(
//                        () -> {
//                            TunnelDataSource dataSource =
//                                    OracleConnect.getInstance()
//                                            .connectViaPortForwarder(
//                                                    credentials.host,
//                                                    credentials.port,
//                                                    credentials.user,
//                                                    credentials.password,
//                                                    credentials.database.orElseThrow(
//                                                            () -> new RuntimeException("Database is required")),
//                                                    TunnelCredentials.fromDbCredentials(
//                                                            credentials,
//                                                            () -> SshKeyService.getSshKey(connectionParams.owner)),
//                                                    false,
//                                                    connectionParams,
//                                                    DB.source());
//
//                            LoggableConfig config =
//                                    new LoggableConfig.Builder()
//                                            .setWritesToCustomerLogs(true)
//                                            .setCloseResourcesQuietly(true)
//                                            .build();
//
//                            return new LoggableDataSource(
//                                    FeatureFlag.check("NoHikariConnectionPool")
//                                            ? dataSource
//                                            : createPoolingDataSource(dataSource),
//                                    config);
//                        });
        return new OracleApi(null);
    }

//    private static HikariDataSource createPoolingDataSource(TunnelDataSource tunnelDataSource) {
//        HikariConfig c = new HikariConfig();
//
//        c.setMaximumPoolSize(5);
//        c.setDataSource(tunnelDataSource);
//
//        return new HikariDataSource(c) {
//            @Override
//            public void close() {
//                super.close();
//                tunnelDataSource.close();
//            }
//        };
//    }

//    @Override
//    public SetupForm<DbCredentials> setupForm(WizardContext wizardContext) {
//        KeyPair keyPair = getSshKey(wizardContext.groupId);
//        return new OracleSetupForm(
//                serviceType,
//                keyPair.getPublicKey(),
//                new ConnectionParameters(wizardContext.groupId, wizardContext.schema));
//    }

    @Override
    public boolean startPaused() {
        return true;
    }

//    @Override
//    public Iterable<WarningType> warningTypes() {
//        return ImmutableList.of(UncommittedWarning.TYPE, NoTableAccessWarning.TYPE, PrimaryKeyChangeWarning.TYPE);
//    }

//    @Override
//    public Iterable<TaskType<I>> taskTypes() {
//        return Arrays.asList(UncommittedTransactionBlockingSync.TYPE, InsufficientFlashbackStorage.TYPE);
//    }

    @Override
    public String schemaRule(Optional<String> schemaNameFromRecord, ConnectionParameters params) {
        return schemaNameFromRecord
                .map(s -> s.replaceFirst(params.schema + "_", ""))
                .orElseThrow(() -> new RuntimeException("Schema name not present in record"));
    }

    @Override
    public boolean supportsTableResync() {
        return true;
    }

    @Override
    public OracleState clearTableState(OracleState state, TableRef table) {
        // Reset table state.
        state.resetTable(table);
        LOG.info(table + " state has been cleared");
        return state;
    }


    @Override
    public StandardConfig standardConfig(DbCredentials creds, ConnectionParameters params, OracleState state) {
        try (OracleApi api = productionApi(creds, params)) {
            StandardConfig result = new StandardConfig();
            result.setSupportsExclude(true);
            SyncMode defaultSyncMode = (FeatureFlag.check("SyncModes")) ? SyncMode.Live : SyncMode.Legacy;

            // TODO: Mark partitioned tables with row movement enabled as NotRecommended due to degraded performance and
            // extra requirements

            return ConnectionFactory.getInstance().retry(
                    "Standard Config",
                    t -> new RuntimeException("Error making Standard Config: ", t),
                    connection -> {
                        if (FeatureFlag.check("OracleFlashback")) {
                            boolean usingUndoLogs = !api.getFlashbackSetupValidator().flashbackDataArchiveEnabled(connection);
                            api.tables()
                                    .forEach(
                                            (table, excluded) -> {
                                                boolean isFlashbackEnabled =
                                                        usingUndoLogs || api.getFlashbackSetupValidator().isTableFlashbackEnabled(connection, table);
                                                TableIncrementalUpdate tableIncrementalUpdate =
                                                        new TableIncrementalUpdate(
                                                                isFlashbackEnabled, "Flashback Data Archive");
                                                ensureConfig(
                                                        table,
                                                        excluded,
                                                        result,
                                                        tableIncrementalUpdate,
                                                        defaultSyncMode);
                                            });
                            return result;
                        } else {
                            boolean isDbSupplementalLoggingEnabled = api.getFlashbackSetupValidator().isSupplementalLoggingEnabled(connection);
                            api.tables()
                                    .forEach(
                                            (table, excluded) -> {
                                                boolean isSupplementalLoggingEnabled =
                                                        isDbSupplementalLoggingEnabled
                                                                || api.getFlashbackSetupValidator().isTableSupplementalLoggingEnabled(
                                                                connection, table);
                                                TableIncrementalUpdate tableIncrementalUpdate =
                                                        new TableIncrementalUpdate(
                                                                isSupplementalLoggingEnabled, "Supplemental logging");
                                                ensureConfig(
                                                        table,
                                                        excluded,
                                                        result,
                                                        tableIncrementalUpdate,
                                                        defaultSyncMode);
                                            });
                            return result;
                        }
                    });
        }
    }


    /**
     * Ensure that there is a config for `table` in `result`
     */
    private void ensureConfig(
            TableRef table,
            Optional<String> excluded,
            StandardConfig result,
            TableIncrementalUpdate tableIncrementalUpdate,
            SyncMode defaultSyncMode) {
        SchemaConfig schemaConfig = (SchemaConfig) result.getSchemas().computeIfAbsent(table.schema, newSchema -> newConfig(table));
        TableConfig tableConfig =
                (TableConfig) schemaConfig.getTables().computeIfAbsent(table.name, newTable -> (TableConfig) TableConfig.withSupportsExclude(true));

        tableConfig.setExcludedBySystem(excluded);
        tableConfig.setSupportsColumnConfig(true);

        tableConfig.setSyncMode(defaultSyncMode);

        if (!tableIncrementalUpdate.enabled) {
            Optional<String> currentlyExcluded = tableConfig.getExcludedBySystem().map(msg -> msg + "; ");
            tableConfig.setExcludedBySystem(
                    Optional.of(
                            currentlyExcluded.orElse("")
                                    + tableIncrementalUpdate.system
                                    + " has not been enabled for this table"));
        }
    }

    /**
     * Generate a new config for `table`
     */
    private SchemaConfig newConfig(TableRef table) {
        SchemaConfig config = new SchemaConfig();

        if (sampleSchemas.contains(table.schema)) {
            config.setNotRecommended(
                    Optional.of(
                            "Sample schema installed by the seed database for documentation and training purposes"));
        }

        return config;
    }

    @Override
    public boolean canDefineSchema() {
        return true;
    }

    //
    @Override
    public Map<String, ColumnConfig> columnConfig(
            DbCredentials creds, ConnectionParameters params, String schema, String table) {
        try (OracleApi api = productionApi(creds, params)) {
            Map<String, ColumnConfig> result = new HashMap<>();

            TableRef tableWithSchema = new TableRef(schema, table);

            Map<String, ColumnConfigInformation> tableColumns = api.tableColumns(tableWithSchema);

            tableColumns
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEachOrdered(
                            e -> {
                                String columnName = e.getKey();
                                boolean isSupported = e.getValue().isSupported;
                                boolean isPrimaryKey = e.getValue().isPrimaryKey;

                                if (!isSupported) {
                                    ColumnConfig config =
                                            ColumnConfig.withExclusionNotSupported(
                                                    Optional.of("Column does not support exclusion"));
                                    config.setExcludedBySystem(Optional.of("Column with unsupported data type"));
                                    result.put(columnName, config);
                                } else {
                                    Optional<String> reason =
                                            isPrimaryKey
                                                    ? Optional.of(
                                                    "Column does not support exclusion as it is a Primary Key")
                                                    : Optional.empty();
                                    result.put(columnName, ColumnConfig.withExclusionNotSupported(reason));
                                }
                            });

            return result;
        }
    }

    @Override
    public Optional<List<Object>> pricingDimensions(
            DbCredentials creds, OracleState state, ConnectionParameters params) {
        Map<String, String> map = new HashMap<>();
        map.put("Host", creds.host);
        map.put("Port", creds.port != null ? creds.port.toString() : "");
//        creds.database.ifPresent(database -> map.put("Database", database));

//        return Optional.of(ImmutableList.of(map));
        return null;
    }

    @Override
    public boolean hidden() {
        return hidden;
    }

    @Override
    public boolean beta() {
        return beta;
    }

    static class TableIncrementalUpdate {
        final Boolean enabled;
        final String system;

        public TableIncrementalUpdate(Boolean enabled, String system) {
            this.enabled = enabled;
            this.system = system;
        }
    }

    @Override
    public Map<String, String> customMicrometerLabels(DbCredentials credentials, ConnectionParameters params) {
        Map<String, String> map = new HashMap<>();

        try {
            String oracleDbVersion =
                    OracleConnect.getInstance()
                            .getOracleDbVersion(productionApi(credentials, params).connectToSourceDb());
            map.put(TagName.DB_VERSION.toString(), oracleDbVersion);
        } catch (Exception e) {
            map.put("db_version", "unknown");
        }

        // Put connection is running on flashback or log-miner
        map.put(
                TagName.UPDATE_STRATEGY.toString(),
                FeatureFlag.check("OracleFlashback") ? IncrementalSyncType.FLASHBACK.toString() : IncrementalSyncType.LOGMINER.toString());

        return map;
    }
}