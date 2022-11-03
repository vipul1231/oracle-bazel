package com.example.db;

import com.example.core.*;
import com.example.core.DbCredentials;
import com.example.core2.Output;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
//import com.example.integrations.speed_test.DbConnectorSpeedMarker;
import com.example.logger.ExampleLogger;
import com.example.logging.progress.DbImportProgress;
import com.example.micrometer.TagValue;
import com.example.oracle.*;
import com.example.snowflakecritic.SnowflakeSourceCredentials;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.setup.SnowflakeConnectorSetupForm;
import com.example.core2.ConnectionParameters;

import com.example.sql_server.SqlServerSource;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import javax.sql.DataSource;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.example.core.SchemaType.DESTINATION_SCHEMA;
import static com.example.core.SchemaType.SOURCE_SCHEMA;
//import static com.example.ssh.SshKeyService.getSshKey;
import static java.util.stream.Collectors.toSet;

/**
 * Defines the standard sync behavior and {@link Service} methods which apply to any DB connector.
 *
 * @param <Creds> - Used by source to authenticate source database connections
 * @param <Source> - Creates source database connections
 * @param <State> - Maintains state between syncs
 * @param <Informer> - Provides metadata for a database
 */
public abstract class DbService<
                Creds extends DbCredentials,
                State,
                TableInfo extends DbTableInfo<?>,
                Source extends AutoCloseable,
                Informer extends DbInformer<Source, TableInfo>>
        implements Service<Creds, State> {

    protected static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public static final Duration INCREMENTAL_SYNC_INTERVAL = Duration.ofHours(1);

    private volatile Instant importPageUntil = Instant.EPOCH;

    final Object importTimingLock = new Object();

    public static boolean debug = false;

    public static boolean isConfigsTrackerUsed = true;

    private final boolean parallelized =
            FeatureFlag.check((String) FlagName.SqlServerParallelizedUpdates) && FeatureFlag.check((String) FlagName.BridgeDecoupledCore);

    @Override
    public ServiceType type() {
        return ServiceType.Database;
    }

//    @Override
    public Set<String> searchKeywords() {
        return serviceType().searchKeywords();
    }

    protected abstract Source source(Creds creds, ConnectionParameters params);

    public abstract Informer informer(Source source, StandardConfig standardConfig);

    public abstract com.example.db.DbImporter<TableRef> importer(Source source, State state, Informer informer, Output<State> out);


    protected abstract com.example.db.DbIncrementalUpdater incrementalUpdater(
            Source source, State state, Informer informer, Output<State> out);

    public abstract DbServiceType serviceType();

//    @Override
    public LinkedHashMap<String, Object> connectionConfig(Creds creds, State state, String owner, String integration) {

        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        map.put("Host", creds.host);
        map.put("Port", creds.port != null ? creds.port.toString() : "");

        creds.database.ifPresent(database -> map.put("Database", database));

        map.put("User", creds.user);
        map.put("Password", "********");
//        map.put("Public Key", getSshKey(owner).getPublicKey());

        creds.tunnelHost.ifPresent(tunnelHost -> map.put("Tunnel Host", tunnelHost));
//        creds.tunnelPort.ifPresent(tunnelPort -> map.put("Tunnel Port", tunnelPort.toString()));
        creds.tunnelUser.ifPresent(tunnelUser -> map.put("Tunnel User", tunnelUser));

        return map;
    }

    protected abstract List<String> tableExcludedReasons(TableInfo tableInfo);

    protected StandardConfig sqlStandardConfig(Creds creds, ConnectionParameters params, State state) {
        StandardConfig standardConfig = new StandardConfig();
//        RenamingRules renamingRules = renamingRules();
        RenamingRules renamingRules = null;
        try (Source source = source(creds, params)) {
            Informer informer = informer(source, standardConfig);
            standardConfig.setSupportsExclude(true);
            Set<TableRef> allTables = informer.allTables();
            /*
             * Build mapping between source and output table names so we can detect naming collisions
             *
             * TODO remove when core handles naming collisions
             */
            Map<TableRef, TableRef> sourceToRenamedDestination = new HashMap<>();
            Map<TableRef, Set<TableRef>> renamedDestinationToSource = new HashMap<>();
            allTables.forEach(
                    tableRef -> {
                        TableRef renamedDestinationTable =
                                new TableRef(tableRef.schema, tableRef.name);
                        sourceToRenamedDestination.put(tableRef, renamedDestinationTable);
                        renamedDestinationToSource.computeIfAbsent(renamedDestinationTable, t -> new HashSet<>());
                        renamedDestinationToSource.get(renamedDestinationTable).add(tableRef);
                    });
            // Put list of source tables into format required by StandardConfig
            allTables.forEach(
                    tableRef -> {
                        TableInfo tableInfo = informer.tableInfo(tableRef);
                        standardConfig.getSchemas().putIfAbsent(tableRef.schema, new SchemaConfig());
//                        standardConfig.getSchema(tableRef.schema).putTable(tableRef.name, new TableConfig());
//                        TableConfig tableConfig = standardConfig.getSchema(tableRef.schema).getTable(tableRef.name);
                        TableConfig tableConfig=null;

                        List<String> excludeReasons = tableExcludedReasons(tableInfo);
                        if (!CollectionUtils.isEmpty(excludeReasons)) {
                            tableConfig.setExcludedBySystem(Optional.of(String.join(", ", excludeReasons)));
                        }

                        // exclude tables whose names would collide in the warehouse due to renaming rules
                        TableRef renamedDestinationTable = sourceToRenamedDestination.get(tableRef);
                        String renamedDestinationTableName = "'" + renamedDestinationTable.name + "'";
                        Set<String> sourceTableNames =
                                renamedDestinationToSource
                                        .get(renamedDestinationTable)
                                        .stream()
                                        .map(st -> "'" + st.name + "'")
                                        .collect(toSet());
                        if (sourceTableNames.size() > 1) {
                            tableConfig.setExcludedBySystem(
                                    Optional.of(
                                            "Naming collision: tables "
                                                    + String.join(" and ", sourceTableNames)
                                                    + " map to the same output table "
                                                    + renamedDestinationTableName));
                        }

                        if (null != tableConfig) {
                            tableConfig.setSupportsColumnConfig(true);
                            tableConfig.setSyncMode(defaultSyncMode());
                        }
                    });
            return standardConfig;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
    public boolean useNewUCMModel() {
        return false;
    }

//    @Override
    public Map<String, ColumnConfig> columnConfig(
            Creds creds, ConnectionParameters params, String schema, String table) {

        try (Source source = source(creds, params)) {
            Informer informer = informer(source, new StandardConfig());

            TableRef tableWithSchema = new TableRef(schema, table);
            Map<String, ColumnConfigInformation> tableColumnConfigInfo =
                    informer.fetchColumnConfigInfo(tableWithSchema);

            return tableColumnConfigInfo
                    .entrySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> {
                                        boolean isSupported = e.getValue().isSupported;
                                        boolean isPrimaryKey = e.getValue().isPrimaryKey;

                                        if (isSupported) {
                                            return null;
//                                            return ColumnConfig.withSupportsExclude(!isPrimaryKey);
                                        } else {
                                            ColumnConfig config = new ColumnConfig();
//                                            config.setExcludedBySystem(
//                                                    Optional.of(
//                                                            e.getValue()
//                                                                    .exclusionReason
//                                                                    .orElse("Column with unsupported data type")));
                                            return config;
                                        }
                                    }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * To be used by connectors that require pre-sync setup. E.g. removing tables that are not found in {@link
     * StandardConfig#includedTables()} from cursors in {@link State}.
     */
    public void prepareSync(
            Source source,
            State state,
            Informer informer,
            com.example.db.DbImporter<TableRef> importer,
            Output<State> out,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables) {

        if (FlagName.ReduceTableSchemaAssertions.check()) {
            List<TableDefinition> tablesStartingImport =
                    includedTables
                            .stream()
                            .filter(t -> !importer.importStarted(t))
                            .map(t -> informer.tableInfo(t).tableDefinition())
                            .collect(Collectors.toList());
//            tablesStartingImport.forEach(out::assertTableSchema);
        } else {
            List<TableDefinition> includedTableDefinitions =
                    includedTables
                            .stream()
                            .map(t -> informer.tableInfo(t).tableDefinition())
                            .collect(Collectors.toList());
//            includedTableDefinitions.forEach(out::assertTableSchema);
        }
    }

//    @Override
    public void update(
            Creds creds,
            State state,
            ConnectionParameters params,
            Output<State> out,
            StandardConfig standardConfig,
            Set<String> featureFlags) {
        TagValue.SyncStatus syncStatus = TagValue.SyncStatus.FAILURE;
        BiConsumer<TagValue.SyncStatus, String> recordImportDuration = (x, y) -> {};
        BiConsumer<TagValue.SyncStatus, String> recordUpdateDuration = (x, y) -> {};
        String dbVersion = "";
        try (Source source = source(creds, params)) {
            if (isConfigsTrackerUsed) runConfigsTracker(source, state);

            Informer informer = informer(source, standardConfig);
            informer.dashboardWarnings().forEach(out::warn);
            dbVersion = informer.version().orElse("");

            prepareTrident(informer, out, state, source);

            com.example.db.DbImporter<TableRef> importer = importer(source, state, informer, out);
            com.example.db.DbIncrementalUpdater incrementalUpdater = incrementalUpdater(source, state, informer, out);

            recordImportDuration = importer::recordImportDuration;
            recordUpdateDuration = incrementalUpdater::recordUpdateDuration;

            Set<TableRef> includedTables = informer.includedTables();
            Set<TableRef> excludedTables = informer.excludedTables();

            prepareSync(source, state, informer, importer, out, includedTables, excludedTables);

            // TODO check - commented the line - sorted(importer.tableSorter)
            List<TableRef> tablesToImport =
                    includedTables
                            .stream()
                            .filter(t -> !importer.importFinished(t))
//                            .sorted(importer.tableSorter())
                            .collect(Collectors.toList());
            Set<TableRef> tablesStartingImport =
                    tablesToImport.stream().filter(t -> !importer.importStarted(t)).collect(Collectors.toSet());
            Set<TableRef> tablesContinuingImport =
                    tablesToImport.stream().filter(importer::importStarted).collect(Collectors.toSet());

            runSpeedMarker(informer, params, creds);

            Runnable incrementalUpdate =
                    () -> {
                        if (FlagName.RunIncrementalSyncOnlyOnImportedTables.check()) {
                            Set<TableRef> importedTables =
                                    includedTables
                                            .stream()
                                            .filter(t -> importer.importStarted(t) || importer.importFinished(t))
                                            .collect(Collectors.toSet());
                            incrementalUpdate(incrementalUpdater, importedTables);
                        } else incrementalUpdate(incrementalUpdater, includedTables);
                    };

            describeSync(informer, includedTables, excludedTables, tablesStartingImport, tablesContinuingImport);

            if (shouldSoftDeleteAfterImport()) {
                // TODO keep with other Table sets when ff removed
                Set<TableRef> tablesRequiringSoftDelete =
                        includedTables
                                .stream()
                                .filter(importer::importFinished)
                                .filter(t -> !importer.softDeletedAfterImport(t))
                                .collect(Collectors.toSet());
                // soft delete any remaining tables in case we fail during DbService#importTable
                tablesRequiringSoftDelete.forEach(t -> softDeleteTable(t, informer, importer));
            } else {
                // FIXME - check
//                tablesStartingImport.forEach(t -> oldSoftDeleteTable(t, informer, out, state));
            }

            doSync(informer, importer, tablesToImport, incrementalUpdate);
            shutdownTrident();
            syncStatus = TagValue.SyncStatus.SUCCESS;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            recordImportDuration.accept(syncStatus, dbVersion);
            recordUpdateDuration.accept(syncStatus, dbVersion);
            cleanUpMetricTimers();
        }
    }

    protected void cleanUpMetricTimers() {
//        for (DbSubPhaseTimerSampler sampler : DbSubPhaseTimerSampler.values()) {
//            sampler.stop();
//        }
    }

    protected void runSpeedMarker(Informer informer, ConnectionParameters params, Creds creds) {
        List<TableInfo> includedInfos =
                informer.includedTables().stream().map(informer::tableInfo).collect(Collectors.toList());
//        if (includedInfos != null && !includedInfos.isEmpty())
//            getSpeedMarker(includedInfos, params, creds).ifPresent(marker -> marker.safelyRun(!debug));
    }

    /** Create an instance of {@link DbConnectorSpeedMarker} to record the connector's theoretical maximum throughput */
//    protected Optional<DbConnectorSpeedMarker> getSpeedMarker(
//            List<TableInfo> targetTable, ConnectionParameters params, Creds creds) {
//        return Optional.empty();
//    }

    /** Prepare Trident object for database integrations that support Trident */
    protected void prepareTrident(Informer informer, Output<State> output, State state, Source source) {
        // no-op
    }

    /**
     * Perform any remaining tasks while the source is still active. Can be used to do things like wait for a Trident
     * Producer thread to spin down while the source is still active.
     */
    protected void shutdownTrident() {
        // no-op
    }

    /**
     * For all existing connectors, the default sync mode is SyncMode.Legacy unless this is overriden and returns a new
     * default.
     */
    protected SyncMode defaultSyncMode() {
        return FeatureFlag.check("SyncModes") ? SyncMode.Live : SyncMode.Legacy;
    }

    protected void doSync(
            Informer informer,
            com.example.db.DbImporter<TableRef> importer,
            List<TableRef> tablesToImport,
            Runnable incrementalUpdate) {
        Set<TableRef> includedTables = informer.includedTables();
        int totalTables = includedTables.size();
        int numImportedTables = totalTables - tablesToImport.size();
        // Refresh the tally before the incremental update (tables may have been added/removed)
        if (!tablesToImport.isEmpty())
            DbImportProgress.logMetricAndUfl(tablesToImport.get(0).toString(), numImportedTables, totalTables);
        for (TableRef t : tablesToImport) {
            DbImportProgress.logMetricAndUfl(t.toString(), numImportedTables++, totalTables);
            importTable(t, informer, importer, incrementalUpdate);

            if (shouldSoftDeleteAfterImport()) {
                softDeleteTable(t, informer, importer);
            }
        }
        incrementalUpdate.run();
    }

    /**
     * Override in subclass to add check for feature flag related to soft deleting after imports (e.g. return
     * FeatureFlag.check("SqlServerSoftDeleteAfterImport")).
     *
     * <p>To avoid accidental feature-flag activation, only invoke FeatureFlag#check in the subclass.
     *
     * <p>// TODO remove method after implemented in all subclasses
     */
    protected boolean shouldSoftDeleteAfterImport() {
        return false;
    }

    protected void describeSync(
            Informer informer,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables,
            Set<TableRef> tablesStartingImport,
            Set<TableRef> tablesContinuingImport) {

        informer.version().ifPresent(v -> DbInfoLoggingUtils.logDbVersion(serviceType(), v));

        String syncDescription =
                String.format(
                        "Tables starting import: %d [estimated total bytes - %s]\n"
                                + "Tables continuing import: %d [estimated total bytes - %s]\n"
                                + "Total tables included in sync: %d\n"
                                + "Total tables excluded from sync: %d\n",
                        tablesStartingImport.size(),
                        totalBytesForTables(informer, tablesStartingImport),
                        tablesContinuingImport.size(),
                        " less than " + totalBytesForTables(informer, tablesContinuingImport),
                        includedTables.size(),
                        excludedTables.size());

//        LOG.customerInfo(InfoEvent.of("sync_description", syncDescription));
    }

    protected void incrementalUpdate(com.example.db.DbIncrementalUpdater incrementalUpdater, Set<TableRef> includedTables) {
//        LOG.customerInfo(InfoEvent.of("incremental_update", "Incrementally updating all included tables"));
        incrementalUpdater.incrementalUpdate(includedTables);
    }

    @Deprecated
    protected void oldSoftDeleteTable(TableRef table, Informer informer, Output<State> out, State state) {
        TableRef destinationTableRef = informer.destinationTableRef(table);

//        LOG.customerInfo(
//                InfoEvent.of(
//                        "soft_delete",
//                        "Initial import or resync detected. Soft deleting any records in " + destinationTableRef));

        out.softDelete(
                destinationTableRef,
                Names.example_DELETED_COLUMN,
                Names.example_SYNCED_COLUMN,
                ExampleClock.Instant.now());

        updateStateAfterOldSoftDelete(table, state);

        out.checkpoint(state);
    }

    /** Override to update state to aid migration to soft delete after import */
    protected void updateStateAfterOldSoftDelete(TableRef table, State state) {}

    protected void softDeleteTable(TableRef table, Informer informer, com.example.db.DbImporter<TableRef> importer) {
        TableRef destinationTableRef = informer.destinationTableRef(table);

//        LOG.customerInfo(
//                InfoEvent.of(
//                        "soft_delete",
//                        "Initial import or resync detected. Soft deleting any records in " + destinationTableRef));

        importer.modeSpecificDelete(table);
    }

    protected void importTable(
            TableRef table, Informer informer, com.example.db.DbImporter<TableRef> importer, Runnable incrementalUpdate) {

//        LOG.customerInfo(
//                InfoEvent.of(
//                        "import_description",
//                        String.format(
//                                "Importing table: %s [estimated total bytes - %s]",
//                                table, totalBytesForTable(informer, table))));
////        LOG.customerInfo(logProgress(informer, importer));
//        LOG.customerInfo(ReadEvent.start(table.toString()));

        while (!importer.importFinished(table)) {
//            LOG.customerInfo(InfoEvent.of("import_page_begin", "Importing page from table " + table));

            importer.importPage(table);
//            LOG.customerInfo(InfoEvent.of("import_page_end", "Finished importing page from table " + table));

            // TODO
            maybeIncrementalUpdate(incrementalUpdate);
        }

//        LOG.customerInfo(ReadEvent.end(table.toString()));
    }

    private void maybeIncrementalUpdate(Runnable incrementalUpdate) {
        if (parallelized) {
            maybeParallelizedIncrementalUpdate(incrementalUpdate);
            return;
        }

        if (FlagName.MysqlTrident.check() || FlagName.SqlServerTrident.check()) {
            incrementalUpdate.run();
        } else if (ExampleClock.Instant.now().isAfter(importPageUntil)) {
            incrementalUpdate.run();
            importPageUntil = ExampleClock.Instant.now().plus(INCREMENTAL_SYNC_INTERVAL);
        }
    }

    private void maybeParallelizedIncrementalUpdate(Runnable incrementalUpdate) {
        boolean runIncrementalSync = false;
        synchronized (importTimingLock) {
            if (ExampleClock.Instant.now().isAfter(importPageUntil)) {
                // ridiculously long time to make sure no other thread starts incrementally updating at the same time
                importPageUntil = ExampleClock.Instant.now().plus(Duration.ofDays(365));
                runIncrementalSync = true;
            }
        }
        if (runIncrementalSync) {
            for (int i = 0; i < 3; i++) {
                try {
                    incrementalUpdate.run();
                    // reset importPageUntil to be a reasonable value once we're done incrementally updating
                    importPageUntil = ExampleClock.Instant.now().plus(INCREMENTAL_SYNC_INTERVAL);
                } catch (RuntimeException e) {
                    if (e.getCause() instanceof SQLTransientConnectionException) {
                        importPageUntil = ExampleClock.Instant.now().plus(INCREMENTAL_SYNC_INTERVAL);
//                        LOG.warning(e.getCause().getMessage());
                    } else throw e;
                }
            }
        }
    }

    /*
    private LogEvent logProgress(Informer informer, com.example.db.DbImporter<TableRef> importer) {
        return ImportProgressEvent.ofSimple(
                informer.includedTables()
                        .stream()
                        .filter(importer::importFinished)
                        .map(TableRef::toString)
                        .collect(Collectors.toSet()),
                informer.includedTables()
                        .stream()
                        .filter(table -> !importer.importFinished(table) && importer.importStarted(table))
                        .map(TableRef::toString)
                        .collect(Collectors.toSet()),
                informer.includedTables()
                        .stream()
                        .filter(table -> !importer.importStarted(table))
                        .map(TableRef::toString)
                        .collect(Collectors.toSet()));
    }
     */

    private String totalBytesForTable(Informer informer, TableRef table) {
        return totalBytesForTables(informer, ImmutableList.of(table));
    }

    private String totalBytesForTables(Informer informer, Collection<TableRef> tables) {
        return tables.stream()
                .map(informer::estimatedSizeInGigaBytes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(Double::sum)
                .map(gigaBytes -> gigaBytes + " GBs")
                .orElse("0.0 GBs");
    }

    @Override
    public boolean targetCore2Directly(Set<String> featureFlags) {
        return true;
    }

    /**
     * Running the configurations tracker to collect the source DB configs info
     *
     * @param source for accessing source DB
     * @param state of a given connector
     */
    protected void runConfigsTracker(Source source, State state) {}

//    @Override
    public String schemaRule(Optional<String> schemaName, ConnectionParameters params, SchemaType schemaType) {
        if (schemaType == SOURCE_SCHEMA)
            return schemaName
                    .map(schema -> schema.replaceFirst(params.schema + "_", ""))
                    .orElseThrow(() -> new RuntimeException("Schema name not present in record"));
        if (schemaType == DESTINATION_SCHEMA) {
            String schemaFromRecord =
                    schemaName.orElseThrow(() -> new RuntimeException("Schema name not present in record"));
            return String.format("%s_%s", params.schema, schemaFromRecord);
        }

        throw new RuntimeException("Invalid Schema type: " + schemaType);
    }

    public abstract SqlServerSource source(DbCredentials credentials, ConnectionParameters params, DataSource dataSource);

    protected abstract List<String> tableExcludedReasons(
            SnowflakeSourceCredentials snowflakeSourceCredentials, SnowflakeTableInfo tableInfo);

    public abstract SnowflakeConnectorSetupForm setupForm(WizardContext wizardContext);

}
