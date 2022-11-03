package com.example.sql_server;


import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.example.core.storage.PersistentStorage;
import com.example.core.storage.PersistentStorageResult;
import com.example.core2.Output;
import com.example.db.DbImporter;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.example.db.StateHistory;

import com.example.oracle.ColumnConfig;
import com.google.common.collect.ImmutableSet;
import org.junit.*;
import org.mockito.InOrder;

import javax.crypto.SecretKey;
import java.io.File;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;
import static com.example.sql_server.SqlServerTridentSpec.createLocalFolder;
import static com.example.sql_server.SqlServerTridentSpec.generateMockSecret;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class SqlServerServiceSpec extends SqlDbServiceSpec<DbCredentials, SqlServerState, SqlServerService> {


    @Rule
    public FeatureFlagHelper flagHelper = new FeatureFlagHelper();

    private static OrderedBucketStorage<ChangeEntry> spiedStorage;

    @BeforeClass
    public static void initializeTridentStorage() {
        String integrationId = UUID.randomUUID().toString();
        SecretKey secretKey = generateMockSecret();

        PersistentStorage ps = mock(PersistentStorage.class);
        when(ps.isEmpty()).thenReturn(true);
        when(ps.getRoot()).thenReturn(integrationId);
        when(ps.download(any(Path.class))).thenReturn(new PersistentStorageResult.Builder<Path>().build());
//        when(ps.download(any(Path.class), argThat(new NonEmptySetMatcher<>()), anyMap()))
//                .thenReturn(new PersistentStorageResult.Builder<Path>().build());
        when(ps.upload(anyMap())).thenReturn(new PersistentStorageResult.Builder<Path>().build());
//        when(ps.delete(anySet())).thenReturn(new PersistentStorageResult.Builder<String>().build());
//        when(ps.purge()).thenReturn(new PersistentStorageResult.Builder<String>().build());

        File directory = createLocalFolder(integrationId);
//        TridentStorage.Builder<ChangeEntry> storageBuilder =
//                new TridentStorage.Builder<>(ps, new ByteBufChangeEntrySerializer(), secretKey)
//                        .withLocalDirectory(directory);

//        spiedStorage = storageBuilder.build();
    }

    @AfterClass
    public static void closeTridentStorage() {
//        spiedStorage.close();
    }

//    @Before
    public void beforeSqlServerV3ServiceSpec() {
        flagHelper.clearFlags();
        flagHelper.addFlags("NoHikariConnectionPool");
//        WarningTypeRegistry.register(new SqlServerService().warningTypes());
//        CaptureConnectorStrategyResyncProvider.reset();
//        CaptureConnectorStrategyResyncProvider.set(() -> mock(CaptureConnectorStrategyResync.class));
    }

    //    @Override
    protected SqlServerService createService() {
        return new SqlServerServiceWithTestTrident();
    }

    //    @Override
    protected void assertCorrectSyncStateHistory(StateHistory<SqlServerState> stateHistory) {
        SqlServerState stateBeforeSync = stateHistory.getEvent(StateHistory.StateEvent.BeforeSync);

        SqlServerState blankState = new SqlServerState();
        assertEquals(blankState.tables, stateBeforeSync.tables);

        SqlServerState stateAfterSync = stateHistory.getEvent(StateHistory.StateEvent.AfterSync);

        assertAfterSyncImportHistory(stateAfterSync);
    }

    //    @Override
    protected void assertCorrectSyncMultiTableStateHistory(StateHistory<SqlServerState> stateHistory) {
        SqlServerState stateAfterSync = stateHistory.getEvent(StateHistory.StateEvent.AfterSync);

        assertEquals(3, stateAfterSync.tables.size());
        assertAfterSyncImportHistory(stateAfterSync);
    }

    private void assertAfterSyncImportHistory(SqlServerState stateAfterSync) {
        assertFalse(stateAfterSync.tables.isEmpty());

        stateAfterSync.tables.forEach(
                (tableRef, tableState) -> {
                    // assert state contents
                    assertTrue("Import should be finished", tableState.importFinished);
//                    assertTrue(
//                            "Import begin time should be after the start of the test",
//                            tableState.importBeginTime.isAfter(testStart));

                    // assert state logic
                    assertTrue(stateAfterSync.isImportFinished(tableRef));
                });
    }

    //    @Override
    protected SqlServerState incrementalUpdateState(List<TableRef> tablesInState) {
        SqlServerState state = new SqlServerState();

        try (SqlServerSource source = new SqlServerSource(testDb.credentials(), params(), testDb.dataSource())) {
//            source.execute(
//                    connection -> {
//                        Long globalSnapshot =
//                                SqlServerIncrementalUpdater.getGlobalSnapshot(
//                                        connection, testDb.credentials().database.get());
//                        BigInteger globalMaxLSN = SqlServerIncrementalUpdater.getGlobalMaxLSN(connection);
//
//                        for (TableRef table : tablesInState) {
//                            state.initTableState(table, Instant.now().minus(Duration.ofHours(1)));
//                            state.setImportFinishedV3(table);
//                            state.updateSnapshotV3(table, Optional.of(globalSnapshot));
//                            state.updateLsn(table, Optional.of(globalMaxLSN));
//                        }
//                    });
        }

        return state;
    }

    @Override
    protected SqlServerState midImportState(Set<TableRef> importStarted, Set<TableRef> importNotStarted) {
        SqlServerState state = newState();

        importNotStarted.forEach(tr -> state.initTableState(tr, Instant.EPOCH));

        importStarted.forEach(
                tr -> {
                    state.initTableState(tr, Instant.EPOCH);
                    state.setImportFinishedV3(tr);
//                    state.tables.get(tr).schemaModifyDate = Instant.MAX;
                });

        return state;
    }

//    @Override
    protected boolean tableIsSoftDeleted(SqlServerState state, TableRef tableRef) {
        return state.tables.get(tableRef).wasSoftDeleted;
    }

    @Override
    protected SqlServerTestDb testDb() {
        return new SqlServerTestDb();
    }

    @Override
    protected DbRowHelper rowHelper() {
        return new SqlServerRowHelper();
    }

    @Override
    protected SqlDbTableHelper tableHelper() {
        return new SqlDbTableHelper();
    }

    @Override
    protected SqlServerState newState() {
        return new SqlServerState();
    }

    @Override
    protected Class<SqlServerState> stateClass() {
        return SqlServerState.class;
    }

    @Override
    protected void executePrepareSync(
            Output<SqlServerState> output,
            SqlServerService service,
            SqlServerState state,
            Set<TableRef> includedTables,
            Set<TableRef> excludedTables) {
//        try (SqlServerSource source = service.source(testDb.credentials(), params(), testDb.dataSource())) {
//            SqlServerInformer informer = service.informer(source, new StandardConfig());
//            DbImporter<TableRef> importer = new SqlServerImporter(source, state, informer, output);
//            service.prepareSync(source, state, informer, importer, output, includedTables, excludedTables);
//        }
    }

    @Test
    public void prepareSqlSync() {
        SqlServerService serviceSpy = spy(createService());
        SqlServerState stateSpy = spy(new SqlServerState());

        try (SqlServerSource source = serviceSpy.source(testDb.credentials(), params(), testDb.dataSource())) {
            SqlServerInformer informer = serviceSpy.informer(source, new StandardConfig());

            serviceSpy.prepareSqlSync(
                    source,
                    stateSpy,
                    informer,
                    new MockOutput2<>(stateSpy),
                    Collections.emptySet(),
                    Collections.emptySet());

            verify(serviceSpy, times(1)).initNewlyIncludedTablesInState(any(SqlServerState.class), any());
            verify(serviceSpy, times(1)).removeExcludedTablesFromState(any(SqlServerState.class), any());
        }
    }

    @Test
    @Ignore
    public void shouldAssertSchema_onSchemaChange() throws SQLException {
        SqlServerService serviceSpy = spy(createService());
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "hello");
        testDb.createTableFromRow(row, defaultTable);

        SqlServerState state = new SqlServerState();
        MockOutput2<SqlServerState> output = spy(new MockOutput2<>(state));

        try (SqlServerSource source = serviceSpy.source(testDb.credentials(), params(), testDb.dataSource())) {
            Runnable prepareSync =
                    () -> {
                        SqlServerInformer informer = serviceSpy.informer(source, new StandardConfig());
                        DbImporter<TableRef> importer = serviceSpy.importer(source, state, informer, output);
                        serviceSpy.prepareSync(
                                source,
                                state,
                                informer,
                                importer,
                                output,
                                ImmutableSet.of(defaultTable),
                                Collections.emptySet());
                    };

            // Initial Sync
            prepareSync.run();
            verify(output, times(1)).assertTableSchema(any());
            state.tables.values().forEach(t -> t.importFinished = true);

            // After import finished
            prepareSync.run();
            verify(output, times(1)).assertTableSchema(any());

            // alter schema
            testDb.execute(
                    "ALTER TABLE " + defaultTable.schema + "." + defaultTable.name + " ADD ADDITIONAL_COLUMN INT");

            prepareSync.run();
            verify(output, times(2)).assertTableSchema(any());
        }
    }

    @Test
    @Ignore
    public void shouldNotAssertSchema_afterImportFinish() throws SQLException {
        SqlServerService serviceSpy = spy(createService());
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "hello");
        testDb.createTableFromRow(row, defaultTable);

        SqlServerState state = new SqlServerState();
        MockOutput2<SqlServerState> output = spy(new MockOutput2<>(state));

//        try (SqlServerSource source = serviceSpy.source(testDb.credentials(), params(), testDb.dataSource())) {
//            Runnable prepareSync =
//                    () -> {
//                        SqlServerInformer informer = serviceSpy.informer(source, new StandardConfig());
//                        DbImporter<TableRef> importer = serviceSpy.importer(source, state, informer, output);
//                        serviceSpy.prepareSync(
//                                source,
//                                state,
//                                informer,
//                                importer,
//                                output,
//                                ImmutableSet.of(defaultTable),
//                                Collections.emptySet());
//                    };
//
//            prepareSync.run();
//            verify(output, times(1)).assertTableSchema(any());
//            state.tables.values().forEach(t -> t.importFinished = true);
//
//            prepareSync.run();
//            verify(output, times(1)).assertTableSchema(any());
//        }
    }

    @Test
    @Ignore
    public void prepareSqlSync_initNewlyIncludedTablesInState() {
        SqlServerService serviceSpy = spy(createService());

        SqlServerState state = new SqlServerState();

        try (SqlServerSource source = serviceSpy.source(testDb.credentials(), params(), testDb.dataSource())) {
            SqlServerInformer informer = serviceSpy.informer(source, new StandardConfig());

            TableRef includedTable = tableHelper.customTable("included_table");
            TableRef excludedTable = tableHelper.customTable("excluded_table");

            serviceSpy.prepareSqlSync(
                    source,
                    state,
                    informer,
                    new MockOutput2<>(state),
                    ImmutableSet.of(includedTable),
                    ImmutableSet.of(excludedTable));

            assertTrue(state.tables.containsKey(includedTable));
            assertFalse(state.tables.containsKey(excludedTable));
        }
    }

    @Test
    @Ignore
    public void prepareSqlSync_removeExcludedTablesFromState() {
        SqlServerService serviceSpy = spy(createService());

        SqlServerState state = new SqlServerState();

        try (SqlServerSource source = serviceSpy.source(testDb.credentials(), params(), testDb.dataSource())) {
            SqlServerInformer informer = serviceSpy.informer(source, new StandardConfig());

            TableRef includedTable = tableHelper.customTable("included_table");
            TableRef excludedTable = tableHelper.customTable("excluded_table");

            state.tables.put(includedTable, new SqlServerState.TableState());
            state.tables.put(excludedTable, new SqlServerState.TableState());

            serviceSpy.prepareSqlSync(
                    source,
                    state,
                    informer,
                    new MockOutput2<>(state),
                    ImmutableSet.of(includedTable),
                    ImmutableSet.of(excludedTable));

            assertTrue(state.tables.containsKey(includedTable));
            assertFalse(state.tables.containsKey(excludedTable));
        }
    }

    @Test
    @Ignore
    public void standardConfig_IncludesTableWithoutFullCDCCoverage() throws SQLException {
        String newCdcExcludedColumn = "untracked_column";

        DbRow<DbRowValue> pkeylessRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", UPDATABLE_COLUMN_NAME).value(3).build(),
                        new DbRowValue.Builder("INT", newCdcExcludedColumn).value(5).build());

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        ((SqlServerTestDb) testDb).disableCDC(defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @captured_column_list = N'%s'",
                        defaultTable.schema, defaultTable.name, UPDATABLE_COLUMN_NAME));
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerService service = spy(createService());
        StandardConfig standardConfig = service.standardConfig(testDb.credentials(), params(), new SqlServerState());

        assertTrue(standardConfig.getSchemas().get(defaultTable.schema).getTables().containsKey(defaultTable.name));
        assertFalse(
                standardConfig
                        .getSchemas()
                        .get(defaultTable.schema)
                        .getTables()
                        .get(defaultTable.name)
                        .getExcludedBySystem()
                        .isPresent());
    }

    @Test
    @Ignore
    public void columnConfig_columnsForCTAndCDCEnabledTable() throws SQLException {
        DbRow<DbRowValue> sqlDbRowValues = rowHelper.simpleRow(1, "1");
        testDb.createTableFromRow(sqlDbRowValues, defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL",
                        defaultTable.schema, defaultTable.name));

        testDb.execute("ALTER TABLE " + defaultTable + " ADD non_cdc_column int");

        SqlServerService service = createService();
        Map<String, ColumnConfig> config =
                service.columnConfig(testDb.credentials(), params(), defaultTable.schema, defaultTable.name);
        for (ColumnConfig c : config.values()) {
//            assertFalse(c.getExcludedBySystem().isPresent());
        }
    }

    @Test
    @Ignore
    public void columnConfig_tableWithoutCT() throws SQLException {
        DbRow<DbRowValue> sqlDbRowValues = rowHelper.simpleRow(1, "1");
        testDb.createTableFromRow(sqlDbRowValues, defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL",
                        defaultTable.schema, defaultTable.name));

        testDb.execute("ALTER TABLE " + defaultTable + " DISABLE CHANGE_TRACKING");
        testDb.execute("ALTER TABLE " + defaultTable + " ADD non_cdc_column int");
        SqlServerService service = createService();
        Map<String, ColumnConfig> config =
                service.columnConfig(testDb.credentials(), params(), defaultTable.schema, defaultTable.name);
//        assertTrue(config.get("non_cdc_column").getExcludedBySystem().isPresent());
    }

    @Test
    @Ignore
    public void columnConfig_columnNotInCDCAreSetExcluded() throws SQLException {
        String newCdcExcludedColumn = "untracked_column";

        DbRow<DbRowValue> pkeylessRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", UPDATABLE_COLUMN_NAME).value(3).build(),
                        new DbRowValue.Builder("INT", newCdcExcludedColumn).value(5).build());

        testDb.createTableFromRow(pkeylessRow, defaultTable);
        ((SqlServerTestDb) testDb).disableCDC(defaultTable);
        testDb.execute(
                String.format(
                        "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s', @role_name = NULL, @captured_column_list = N'%s'",
                        defaultTable.schema, defaultTable.name, UPDATABLE_COLUMN_NAME));
        testDb.insertRowIntoTable(pkeylessRow, defaultTable);

        SqlServerService service = spy(createService());
        Map<String, ColumnConfig> config =
                service.columnConfig(testDb.credentials(), params(), defaultTable.schema, defaultTable.name);

//        assertFalse(
//                "ExcludedBySystem for a supported column must be empty in the ColumnConfig",
//                config.get(UPDATABLE_COLUMN_NAME).getExcludedBySystem().isPresent());
//        assertTrue(
//                "ExcludedBySystem for a column not in CDC must be present in the ColumnConfig",
//                config.get(newCdcExcludedColumn).getExcludedBySystem().isPresent());
    }

    @Test
    @Ignore
    public void columnConfig_unsupportedTypesAreSetExcluded() throws SQLException {
        DbRow<DbRowValue> notFullySupportRow =
                new DbRow<>(
                        new DbRowValue.Builder("INT", "supported_col").inValue(1).primaryKey(true).build(),
                        new DbRowValue.Builder("SQL_VARIANT", "unsupported_col").inValue("null").build());

        testDb.createTableFromRow(notFullySupportRow, defaultTable);

        SqlServerService service = spy(createService());

        Map<String, ColumnConfig> config =
                service.columnConfig(testDb.credentials(), params(), defaultTable.schema, defaultTable.name);

//        assertFalse(
//                "ExcludedBySystem for a supported column must be empty in the ColumnConfig",
//                config.get("supported_col").getExcludedBySystem().isPresent());
//        assertTrue(
//                "ExcludedBySystem for an unsupported column must be present in the ColumnConfig",
//                config.get("unsupported_col").getExcludedBySystem().isPresent());
    }

    @Test
    @Ignore
    public void runSpeedMarker() {
        flagHelper.addFlags("EnableSqlServerSpeedMarker");
        SqlServerService service = (createService());

        SqlServerInformer informer = mock(SqlServerInformer.class);
        when(informer.includedTables()).thenReturn(Collections.EMPTY_SET);
        InOrder inOrderVariable = inOrder(informer);
//        service.runSpeedMarker(informer, params(), testDb.credentials());
        inOrderVariable.verify(informer).includedTables();
        inOrderVariable.verifyNoMoreInteractions();
    }

    @Test
    @Ignore
    public void getSpeedMarker() {
        SqlServerService service = (createService());
//        assertThat(service.getSpeedMarker(Collections.EMPTY_LIST, params(), testDb.credentials())).isEmpty();
    }

    @Test
    @Ignore
    public void service_testTridentSyncWithLegacyTable() throws Exception {
//        FeatureFlag.setupForTest("NoHikariConnectionPool", FlagName.SqlServerTrident.name());

        TableRef legacyModeTable = new TableRef(defaultTable.schema, "legacy_mode_table");

        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "hello");
        testDb.createTableFromRow(row, legacyModeTable);

//        SqlServerState state = incrementalUpdateState(legacyModeTable);

//        state.tables.get(legacyModeTable).importFinished = false;

        testDb.insertRowIntoTable(row, legacyModeTable);

//        StandardConfig standardConfig = tableHelper.createStandardConfig(legacyModeTable);
//        MockOutput2<SqlServerState> out = new MockOutput2<>(state);
        SqlServerService service = createService();

//        try (SqlServerSource source = service.source(testDb.credentials(), params(), testDb.dataSource())) {
//            SqlServerInformer informer = service.informer(source, standardConfig);
//
//            service.prepareTrident(informer, out, state, source);
//
//            await().until(spiedStorage::hasMore);
//
            // Flush storage before reading from Trident
//            spiedStorage.close();

            // We are forcing the doSync to only preform a single trident read
//            service.doSync(informer, null, Collections.emptyList(), () -> {
//            });

            service.shutdownTrident();
//        }

//        RecordComparator.compareDbConnectorOutput(out, this.getClass());
    }

    @Test
    @Ignore
    public void renamingRules() {
        SqlServerService service = createService();

//        assertEquals(RenamingRules.DbRenamingRules(), service.renamingRules());
    }

    private static class SqlServerServiceWithTestTrident extends SqlServerService {
        @Override
        SqlServerTrident createTrident(
                SqlServerSource source, SqlServerState state, SqlServerInformer informer, Output<SqlServerState> out) {
            return new SqlServerTrident(source, state, informer, out, spiedStorage, Duration.ZERO);
        }
    }

    @Test
    @Ignore
    public void prepareSqlSync_ShouldGiveWarning_IfNotFullyMigrated() throws Exception {
//        flagHelper.addFlags(FlagName.SyncModes.name());

        TableRef legacyModeTable = new TableRef(defaultTable.schema, "legacy_mode_table");
        TableRef liveModeTable = new TableRef(defaultTable.schema, "live_mode_table");

        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "hello");
        testDb.createTableFromRow(row, legacyModeTable);
        testDb.createTableFromRow(row, liveModeTable);

        SqlServerState state = newState();
        MockOutput2<SqlServerState> output = new MockOutput2<>(state);
        SqlServerService service = createService();
        Map<TableRef, SyncMode> syncModesByTable = new HashMap<>();
        syncModesByTable.put(legacyModeTable, SyncMode.Legacy);
        syncModesByTable.put(liveModeTable, SyncMode.Live);

        StandardConfig standardConfig = tableHelper.createStandardConfig(syncModesByTable);
        service.update(testDb.credentials(), state, params(), output, standardConfig, Collections.emptySet());

//        assertEquals(1, output.warns.size());
//        assertEquals("sync_modes_migration_incomplete", output.warns.get(0).getType().type());
    }

    @Ignore // TODO: Flaky test, moved to SqlServerIncrementalUpdaterCdcSpec
    @Test
    @Override
    public void sync_incrementalUpdates_primaryKeylessAllTypes() throws Exception {
        super.sync_incrementalUpdates_primaryKeylessAllTypes();
    }
}
