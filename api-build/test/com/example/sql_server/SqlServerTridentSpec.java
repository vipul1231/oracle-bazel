package com.example.sql_server;

//import static com.example.integrations.sql_server.Operation.*;
import static com.example.sql_server.Operation.CT_DELETE;
import static com.example.sql_server.Operation.INSERT;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
//import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.example.core.*;
import com.example.core.mocks.MockOutput2;
//import com.example.crypto.DataKey;
//import com.example.crypto.MockMasterKeyEncryptionService;
//import com.example.integrations.sql_server.exceptions.ChangeTrackingException;
//import com.example.integrations.trident.*;
//import com.example.integrations.trident.providers.MockPersistentStorageContext;
//import com.example.json.CopyOf;
//import com.example.json.DefaultObjectMapper;
import com.example.utils.ExampleClock;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.SecretKey;

import org.junit.*;

public class SqlServerTridentSpec {

    private final String integrationId = UUID.randomUUID().toString();
    private final TableRef table = new TableRef("test_schema", "test_table");

    private SqlServerState state;
    private SqlServerInformer informer;
    private MockOutput2<SqlServerState> out;
    private SqlServerTrident trident;
    private OrderedBucketStorage<ChangeEntry> storage;

    @Before
    public void setup() {
//        TridentStorage.Builder<ChangeEntry> mockTridentBuilder =
//                new TridentStorage.Builder<>(
//                        new MockPersistentStorageContext(integrationId).getPersistentStorage(),
//                        new ByteBufChangeEntrySerializer(),
//                        generateMockSecret())
//                        .withLocalDirectory(createLocalFolder(integrationId));
//        storage = spy(mockTridentBuilder.build());

        // The following will enable Trident producer thread (i.e. there is a table that hasn't finished importing)
        state = new SqlServerState();
        state.initTableState(table, Instant.now());
        informer = mock(SqlServerInformer.class);
        when(informer.includedTables()).thenReturn(Collections.singleton(table));

        out = spy(new MockOutput2<>(state));
        trident =
                spy(
                        new SqlServerTrident(null, state, informer, out, storage, Duration.ZERO) {
                            @Override
                            Optional<Long> maybeDelayNextRun(Instant lastRun) {
                                return Optional.of(0L);
                            }
                        });
    }

    @After
    public void teardown() {
        storage.close();
    }

    @Test
    public void serializeTridentState() {
        SqlServerState state = new SqlServerState();
//        state.setDataKeyEncryptedKey(Base64.getEncoder().encodeToString(new byte[] {0, 1, 2, 3, 4, 5}));
        String storageId = state.getTridentStorageId();
//        state.setReadCursors(Collections.singletonMap(table, new TridentCursor(3, 1234L)));

//        SqlServerState copy = CopyOf.state(state);
        SqlServerState copy = null;

        assertEquals(state, copy);
        assertEquals(storageId, copy.getTridentStorageId());
    }

    @Test
    public void testDesupported_setEncryptedKeyMethod() throws Exception {
        String serializedState =
                "{\"tables\": {\"uwbtiqww.ebohfxwx\": {\"key\": null, \"lsn\": null, \"snapshot\": 34, \"importFinished\": true, "
                        + "\"wasSoftDeleted\": true, \"importBeginTime\": \"2021-05-14T22:20:40.497Z\"}, \"uwbtiqww.wqywhqpl\": "
                        + "{\"key\": null, \"lsn\": null, \"snapshot\": 34, \"importFinished\": true, "
                        + "\"wasSoftDeleted\": true, \"importBeginTime\": \"2021-05-14T22:20:40.497Z\"}}, "
                        + "\"readCursors\": {}, \"encryptedKey\": \"AAECAwQF\", "
                        + "\"tridentStorageId\": \"311c2ee1-c85d-41f5-a040-c336463fb016\"} ";

//        SqlServerState state = DefaultObjectMapper.create().readValue(serializedState, SqlServerState.class);
//        assertEquals("AAECAwQF", state.getDataKeyEncryptedKey());
    }

    @Test
    public void trident_read_rescheduleIfStorageIsFull() {
        doReturn(true).when(storage).isFull();
        SqlServerTrident testTrident =
                new SqlServerTrident(null, state, informer, out, storage, Duration.ofMillis(200)) {
                    @Override
                    boolean maybeRescheduleSync(Instant lastRun) {
                        return super.maybeRescheduleSync(
                                ExampleClock.Instant.now().minus(MAX_IS_FULL_WAIT_DURATION).minusSeconds(1));
                    }
                };

        testTrident.startProducer(() -> {});
//        assertThrows(Rescheduled.class, testTrident::read);
    }

    @Test
    public void shouldPropagateException() {
        AtomicBoolean ready = new AtomicBoolean(false);

        trident.startProducer(
                () -> {
                    ready.set(true);
                    throw new RuntimeException("Runtime exception occurred");
                });
        await().until(ready::get);
//        assertThrows(RuntimeException.class, () -> trident.read());
//        assertThrows(RuntimeException.class, () -> trident.stopProducer());

        ready.set(false);
        trident.startProducer(
                () -> {
                    ready.set(true);
                    throw new IllegalStateException("Illegal state exception occurred");
                });
        await().until(ready::get);
//        assertThrows(RuntimeException.class, () -> trident.read());
//        assertThrows(RuntimeException.class, () -> trident.stopProducer());

        ready.set(false);
        trident.startProducer(
                () -> {
                    ready.set(true);
//                    throw CompleteWithTask.enableDatabaseChangeTracking(
//                            "db", new ChangeTrackingException("CT exception"));
                });
        await().until(ready::get);
//        assertThrows(CompleteWithTask.class, () -> trident.read());
//        assertThrows(CompleteWithTask.class, () -> trident.stopProducer());
    }

    @Test
    public void waitUntilNotFull() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        SqlServerTrident testTrident =
                new SqlServerTrident(null, state, informer, out, storage, Duration.ofMillis(200)) {
                    @Override
                    boolean maybeRescheduleSync(Instant lastRun) {
                        return false;
                    }
                };

        doReturn(true).when(storage).isFull();
        testTrident.startProducer(
                () -> {
                    count.getAndIncrement();
                    await().until(() -> false); // block
                });
        Thread.sleep(1_000);
        assertEquals(0, count.get());

        doReturn(false).when(storage).isFull();
        Thread.sleep(500);
        testTrident.stopProducer();

        assertEquals(1, count.get());
    }

    @Test
    public void trident_storageTest() {
        int numOfChanges = 100;
        AtomicLong ctVersion = new AtomicLong(0);
        AtomicBoolean stop = new AtomicBoolean(false);
        TableRef testTable = new TableRef("test_schema", "test_table");

        trident.startProducer(
                () -> {
                    if (ctVersion.get() == numOfChanges) {
                        stop.set(true);
                        return;
                    }

                    storage.add(
                            table, ChangeEntry.v1(INSERT, null, ctVersion.getAndIncrement(), null, new HashMap<>()));
                });

        await().until(stop::get);

        trident.stopProducer();
        storage.close(); // Close to finalize batch files

//        try (TridentIterator<ChangeEntry> iterator = spy(storage.iterator(new HashMap<>()))) {
//            while (iterator.hasNext()) {
//                Map.Entry<TableRef, ChangeEntry> entry = iterator.next();
//                assertEquals(entry.getKey(), testTable);
//            }
//
//            verify(iterator, times(numOfChanges)).next();
//        }
    }

    @Test
    public void waitUntilProducerStops() {
        AtomicInteger count = new AtomicInteger();

        trident.startProducer(
                () -> {
                    try {
                        Thread.sleep(100);
                        count.getAndIncrement();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });

        trident.read();
        trident.stopProducer();

        assertTrue(count.get() > 10);
        verify(trident, atLeast(1)).checkProducerIsHealthy();
    }

    @Test
    public void trident_read_exitWhenStorageIsEmpty() {
        doReturn(false).when(storage).hasMore();
        doReturn(false).when(trident).isProducerEnabled();

        trident.read();

        verify(storage).hasMore();
        verifyNoMoreInteractions(storage);
    }

    @Test
    public void trident_read_legacyModeEntryWithinTrident() {
        doNothing().when(trident).checkProducerIsHealthy();

        ChangeEntry legacyChange = ChangeEntry.v1(INSERT, null, 0L, null, ImmutableMap.of("id", 0));
        storage.add(table, legacyChange);
        storage.close(); // Close to finalize batch files

        SqlServerTableInfo tableInfo = tableInfo(SyncMode.Legacy);
        doReturn(tableInfo).when(informer).tableInfo(table);

        trident.read();

        verify(out).upsert(eq(tableInfo.tableDefinition()), eq(legacyChange.values));
    }

    @Test
    public void trident_read_historyModeEntryWithinTrident() {
        doNothing().when(trident).checkProducerIsHealthy();

        ChangeEntry historyChange = ChangeEntry.v1(INSERT, Instant.now(), 0L, null, ImmutableMap.of("id", 0));
        storage.add(table, historyChange);
        storage.close(); // Close to finalize batch files

        SqlServerTableInfo tableInfo = tableInfo(SyncMode.History);
        doReturn(tableInfo).when(informer).tableInfo(table);

        trident.read();

        verify(out).upsert(eq(tableInfo.tableDefinition()), eq(historyChange.values), eq(historyChange.opTime));
    }

    @Test
    public void trident_read_useOutputUpdateForCTDelete() {
        doNothing().when(trident).checkProducerIsHealthy();

        ChangeEntry ctUpdate = ChangeEntry.v1(CT_DELETE, null, 0L, null, ImmutableMap.of("id", 0));
        storage.add(table, ctUpdate);
        storage.close(); // Close to finalize batch files

        SqlServerTableInfo tableInfo = tableInfo(SyncMode.Legacy);
        doReturn(tableInfo).when(informer).tableInfo(table);

        trident.read();

        verify(out).update(eq(tableInfo.tableDefinition()), eq(ctUpdate.values));
    }

    static SecretKey generateMockSecret() {
//        GroupMasterKeyService groupMasterKeyService = new MockMasterKeyEncryptionService();

//        String mockEncryptionContext = "mock context";
//        DataKey dataKey = groupMasterKeyService.newDataKey(mockEncryptionContext);

//        byte[] encryptedKey = dataKey.encryptedKey;
//
//        return new SecretKeySpec(groupMasterKeyService.decryptDataKey(encryptedKey, mockEncryptionContext), "AES");
        return null;
    }

    static File createLocalFolder(String persistentStorageId) {
        try {
            return Files.createTempDirectory(persistentStorageId + "-").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create local folder");
        }
    }

    private SqlServerTableInfo tableInfo(SyncMode syncMode) {
//        return new SqlServerTableInfo(
//                table,
//                "wh_test",
//                0,
//                0,
//                Collections.emptyList(),
//                0,
//                Collections.emptySet(),
//                SqlServerChangeType.CHANGE_TRACKING,
//                null,
//                false,
//                null,
//                syncMode,
//                null);
        return null;
    }
}
