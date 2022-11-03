package com.example.sql_server;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * This test case is for demonstrating the fixes to the OutofOrder issues
 * in Trident updates
 */
public class SqlServerTridentOutofOrderFixSpecEx {

    private static final String UPDATER = "INCREMENTAL_UPDATER";
    private static final String IMPORTER = "IMPORTER";
    private static final String TRIDENT = "TRIDENT";

    private final TableRef table = new TableRef("test_schema", "test_table");
    private SqlServerState state;
    private SqlServerInformer informer;
    private SqlServerTrident trident;
    private MockOutput2<SqlServerState> out;
    private OrderedBucketStorage<ChangeEntry> storage;
    private TridentIterator tridentIterator;

    Instant updatedStartTime;
    Instant importerStartTime;
    private static List<Operation> operations = new ArrayList<>();

    class Operation {
        public Operation(String tableName, String source, Instant time) {
            this.tableName = tableName;
            this.source = source;
            this.time = time;
        }

        public Operation() {
        }

        private String tableName;
        private String source;
        private Instant time;

        public String getTableName() {
            return tableName;
        }

        public String getSource() {
            return source;
        }

        public Instant getTime() {
            return time;
        }

        @Override
        public String toString() {
            return "Operation{" +
                    "tableName='" + tableName + '\'' +
                    ", source='" + source + '\'' +
                    ", time=" + time +
                    '}';
        }
    }

    @Before
    public void setup() {
        storage = spy(OrderedBucketStorage.class);
        tridentIterator = spy(TridentIterator.class);

        state = new SqlServerState();


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
        doNothing().when(trident).checkProducerIsHealthy();

        when(informer.tableInfo(any())).thenReturn(tableInfo(SyncMode.History));


        doAnswer(call -> {
            operations.add(new Operation(table.name, TRIDENT, call.getArgument(2)));
            return null;
        }).when(out).upsert(any(), any(), any());

        when(storage.hasMore()).thenAnswer(invocation -> {
            return operations.isEmpty();
        });
        when(tridentIterator.hasNext()).thenAnswer(invocation -> {
            return operations.isEmpty();
        });
        when(storage.iterator(any(), any())).thenReturn(tridentIterator);

    }

    /**
     * If importer is started
     * if updater time is greater than importer started time, reset the time to importer started time
     */
    @Test
    public void testUpsertIfImporterStarted() {
        reset();

        importerStartTime = Instant.now();
        state.initTableState(table, importerStartTime);
        updatedStartTime = importerStartTime.plus(10, ChronoUnit.SECONDS);

        when(tridentIterator.next()).thenAnswer(invocation -> {
            Map.Entry<TableRef, ChangeEntry> mapEntry = new AbstractMap.SimpleEntry(table, ChangeEntry.v1(null, updatedStartTime, 1L, null, new HashMap<>()));
            return mapEntry;
        });

        trident.read();
        // Use import begin time, not finish time, to discard update record before import start
        Assert.assertTrue("Use import begin time, not finish time, to discard update record before import start", !operations.isEmpty());

        // Reset operation time to import begin time, when import has NOT been finished
        Assert.assertTrue("Reset operation time to import begin time, when import has NOT been finished", operations.get(0).getTime().equals(importerStartTime));
    }

    private void reset() {
        operations.clear();
    }

    /**
     * If importer is Finished
     * We don't reset the time. It will be the Optime from CDC tables
     */
    @Test
    public void testUpsertIfImporterFinished() {
        reset();

        importerStartTime = Instant.now();
        state.initTableState(table, importerStartTime);
        updatedStartTime = importerStartTime.plus(10, ChronoUnit.SECONDS);
        state.setImportFinishedV3(table);

        when(tridentIterator.next()).thenAnswer(invocation -> {
            Map.Entry<TableRef, ChangeEntry> mapEntry = new AbstractMap.SimpleEntry(table, ChangeEntry.v1(null, updatedStartTime, 1L, null, new HashMap<>()));
            return mapEntry;
        });

        trident.read();

        Assert.assertTrue("Don't skip records on any scenario, so operations has to be populated", !operations.isEmpty());
        Assert.assertTrue("Importer already finished. Do not reset. Use the Optime from CDC tables", operations.get(0).getTime().equals(updatedStartTime));
    }

    private SqlServerTableInfo tableInfo(SyncMode syncMode) {
        return new SqlServerTableInfo(
                table,
                "wh_test",
                0,
                0,
                Collections.emptyList(),
                0,
                Collections.emptySet(),
                SqlServerChangeType.CHANGE_TRACKING,
                null,
                false,
                null,
                syncMode);
    }
}
