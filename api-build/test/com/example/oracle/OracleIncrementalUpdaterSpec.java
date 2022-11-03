package com.example.oracle;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import com.example.db.MockResultSet;
import com.example.lambda.SetOnce;
import com.example.oracle.logminer.LogMinerContentsDao;
import com.example.oracle.logminer.OracleLogMinerIncrementalUpdater;
import com.example.oracle.warnings.UncommittedWarning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.example.oracle.LogMinerOperation.*;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/13/2021<br/>
 * Time: 10:48 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleIncrementalUpdaterSpec extends AbstractOracleTestSpec {

    private OracleApi api;
    private DataSource dataSource;
    private OracleLogMinerIncrementalUpdater oracleLogMinerIncrementalUpdater;
    private LogMinerContentsDao logMinerContentsDao;
    private OracleConnectorContext context;

    //    @Rule public FeatureFlagHelper flagHelper = new FeatureFlagHelper();
    private static final TableRef MOCK_TABLE = new TableRef("S", "T");
    private static final Set<TableRef> SELECTED_TABLES = ImmutableSet.of(MOCK_TABLE);

    @Before
    public void beforeOracleIncrementalUpdaterSpec() {
        dataSource = mock(DataSource.class);
        api = spy(new OracleApi(dataSource));
        context = mock(OracleConnectorContext.class);
        oracleLogMinerIncrementalUpdater = mock(OracleLogMinerIncrementalUpdater.class);
        logMinerContentsDao = mock(LogMinerContentsDao.class);

        Transactions.sleepDurationFunction = Transactions.quickSleepDurationFunction; // speed up tests with retry logic
    }

    private Map<String, LogMinerOperation> completedTransaction(ResultSet resultSet) throws SQLException {
        Map<String, LogMinerOperation> completed = new HashMap<>();

        logMinerContentsDao.readCompletedTransactions(
                resultSet,
                (xid, op) -> {
                    if (op == COMMIT_XID || op == ROLLBACK_XID) completed.put(xid, op);
                });

        return completed;
    }

    @Test
    public void testRowChunking() {
        Boolean[] rows =
                ("tfttffftft"
                        + "tfttffftft"
                        + "tfttffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffff"
                        + "ffffffffft")
                        .chars()
                        .boxed()
                        .map(c -> c == 't')
                        .toArray(Boolean[]::new);
        mockConnection();
        Transactions.ChangedRowsResult res =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0,
                        rows.length,
                        (conn, start, end, more) -> {
                            long last = -1;
                            for (long i = start; i < end; i++) if (i < rows.length && rows[(int) i]) last = i;
                            return last;
                        },
                        __ -> {
                        },
                        Optional.empty());
        assertTrue("Made appropriate amount of progress", res.earliestUncommittedScn >= (long) (rows.length * 0.95));
    }

    @Test
    public void processRowChunksIncreaseScnInterval_failure() {
        mockConnection();
        SetOnce<Boolean> hasFailed = new SetOnce<>();
        SetOnce<Boolean> hasFailedAgain = new SetOnce<>();
        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart() - 1, // one page
                        (conn, startScn, endScn, more) -> {
                            if (hasFailed.isPresent()) {
                                if (hasFailedAgain.isPresent()) {
                                    return endScn + 1L;
                                }
                                hasFailedAgain.set(true);
                                throw new SQLRecoverableException("Some sort of timeout error!");
                            }
                            hasFailed.set(true);
                            throw new SQLRecoverableException("Some sort of timeout error!");
                        },
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        long expectedScnInterval =
                (api.pageSizeConfig.get().getStart() / 2) // 3 tries, 2 failures, so page is halved
                        + api.pageSizeConfig.get().getStep();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_singlePage() {
        mockConnection();
        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart(),
                        (conn, startScn, endScn, more) -> endScn + 1L,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // default + base step up
        long expectedScnInterval = api.pageSizeConfig.get().getStart() + api.pageSizeConfig.get().getStep();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_singlePageNoProgressWithNoChanges_whileLoopCondition() {
        mockConnection();
        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart() * 2,
                        (conn, startScn, endScn, more) -> startScn,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // default (no progress step should not still be there)
        long expectedScnInterval = api.pageSizeConfig.get().getStart();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_singlePageNoProgressWithNoChanges_closeToEndCondition() {
        mockConnection();
        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        (long) (api.pageSizeConfig.get().getStart() * 2.02),
                        (conn, startScn, endScn, more) -> startScn,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // default (no progress step should not still be there)
        long expectedScnInterval = api.pageSizeConfig.get().getStart();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_singlePageNoProgressWithChanges() {
        mockConnection();
        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart() * 2,
                        // first run will return no changes, second run will return success
                        (conn, startScn, endScn, more) ->
                                endScn == api.pageSizeConfig.get().getStart() ? startScn : endScn + 1,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // default + step (last page is less than page size, so no increase)
        long expectedScnInterval = api.pageSizeConfig.get().getStart() + api.pageSizeConfig.get().getStep();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_multiPage_eachPageIsFaster() {
        mockConnection();

        long numPages = 5;

        doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    long startScn = (long) args[0];
                    long pageSize = (long) args[3];
                    long endScn = startScn + pageSize + 1;
                    long nanosPerScn = Integer.MAX_VALUE - endScn; // faster each time
                    return new Transactions.ProcessRowChunkPageResult(endScn, endScn, pageSize, nanosPerScn);
                })
                .when(oracleLogMinerIncrementalUpdater)
                .processRowChunkPage(anyLong(), anyLong(), any(), anyLong(), anyLong(), anyBoolean());

        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart() * (numPages + 1),
                        (__, ___, ____, _____) -> 1,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // default + (base step up * number of pages)
        long expectedScnInterval =
                api.pageSizeConfig.get().getStart() + (api.pageSizeConfig.get().getStep() * (numPages));
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }

    @Test
    public void processRowChunksIncreaseScnInterval_multiPage_eachPageIsSlower() {
        mockConnection();

        doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    long startScn = (long) args[0];
                    long pageSize = (long) args[3];
                    long endScn = startScn + pageSize + 1;
                    long nanosPerScn = endScn; // slower each time
                    return new Transactions.ProcessRowChunkPageResult(endScn, endScn, pageSize, nanosPerScn);
                })
                .when(oracleLogMinerIncrementalUpdater)
                .processRowChunkPage(anyLong(), anyLong(), any(), anyLong(), anyLong(), anyBoolean());

        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart() * 4,
                        (__, ___, ____, _____) -> 1,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // first page will step up (it always does)
        // all other pages will step down
        long expectedScnInterval =
                api.pageSizeConfig.get().getStart()
                        + api.pageSizeConfig.get().getStep()
                        - api.pageSizeConfig.get().getStep()
                        - api.pageSizeConfig.get().getStep()
                        - api.pageSizeConfig.get().getStep();
        assertEquals("Result SCN Interval", expectedScnInterval, changedRowsResult.lastPageSize);
    }


    @Test
    public void processRowChunksIncreaseScnInterval_minValue() {
        mockConnection();

        doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    long startScn = (long) args[0];
                    long pageSize = (long) args[3];
                    long endScn = startScn + pageSize + 1;
                    long nanosPerScn = endScn; // slower each time
                    return new Transactions.ProcessRowChunkPageResult(endScn, endScn, pageSize, nanosPerScn);
                })
                .when(oracleLogMinerIncrementalUpdater)
                .processRowChunkPage(anyLong(), anyLong(), any(), anyLong(), anyLong(), anyBoolean());

        Transactions.ChangedRowsResult changedRowsResult =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0L,
                        api.pageSizeConfig.get().getStart()
                                * (api.pageSizeConfig.get().getStart() / api.pageSizeConfig.get().getMin()),
                        (__, ___, ____, _____) -> 1,
                        __ -> {
                        },
                        Optional.of(api.pageSizeConfig.get().getStart()));

        // This will step down repeatedly, but we should always end up with api.pageSizeConfig.get().getMin() at a
        // minimum
        assertEquals("Result SCN Interval", api.pageSizeConfig.get().getMin(), changedRowsResult.lastPageSize);
    }

    public void skipUncommittedTransactionsInner(List<Map<String, Object>> rows) throws SQLException {

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
//        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedJailTableWarning) warning).xid));

//        doReturn(true).when(api).isScnOlderThanUpperBoundInterval(anyLong());
//        doReturn(true).when(api).isTrueUncommittedTransaction(any());
        api.setOutputHelper(
                new OracleOutputHelper(
                        new MockOutput2<>(new OracleState()), "dest", new StandardConfig(), new OracleMetricsHelper()));

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 6L, endScn);
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_2", "ROW_ID_3")));
        assertEquals(1, caughtRows.size());
    }

    @Test
    public void skipUncommittedRollbackTransactions() throws SQLException {
//        flagHelper.addFlags("OracleSaveUncommittedTransactionsToJailTable");
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());
        skipUncommittedTransactionsInner(rows);
    }

    @Test
    public void skipUncommittedPendingTransactions() throws SQLException {
//        flagHelper.addFlags("OracleSaveUncommittedTransactionsToJailTable");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3").build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());
        skipUncommittedTransactionsInner(rows);
    }

    private void skipCommittedTransactionsWithSQLGreaterThan4000CharsInner(List expected) throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
//                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2").addCsf(1).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001133", "").build());

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
//        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedJailTableWarning) warning).xid));

//        doReturn(false).when(api).isScnOlderThanUpperBoundInterval(anyLong());
//        doReturn(false).when(api).isTrueUncommittedTransaction(any());
        api.setOutputHelper(
                new OracleOutputHelper(
                        new MockOutput2<>(new OracleState()), "dest", new StandardConfig(), new OracleMetricsHelper()));

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 4L, endScn);
        assertThat(rowIds, equalTo(expected));
        assertEquals(0, caughtRows.size());
    }

    @Test
    public void skipCommittedTransactionsWithSQLGreaterThan4000CharsWithFlag() throws SQLException {

//        flagHelper.addFlags("OracleSaveUncommittedTransactionsToJailTable");
        skipCommittedTransactionsWithSQLGreaterThan4000CharsInner(ImmutableList.of("ROW_ID_2"));
    }

    @Test
    public void skipCommittedTransactionsWithSQLGreaterThan4000Chars() throws SQLException {

        skipCommittedTransactionsWithSQLGreaterThan4000CharsInner(ImmutableList.of("ROW_ID_2", "ROW_ID_2"));
    }

    @Test
    public void syncShouldStopAtUncommittedTransactionsIfpendingLessThenUpperBound() throws SQLException {
//        flagHelper.addFlags("OracleSaveUncommittedTransactionsToJailTable");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3").build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());

//        doReturn(false).when(api).isScnOlderThanUpperBoundInterval(anyLong());
        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        api.setOutputHelper(
                new OracleOutputHelper(
                        new MockOutput2<>(new OracleState()), "dest", new StandardConfig(), new OracleMetricsHelper()));

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 2L, endScn);
        assertThat(rowIds, equalTo(ImmutableList.of()));
        // One rows should have been skipped.
        assertEquals(1, caughtRows.size());
    }

    @Test
    public void uncommittedTransactionContainingRollbacksIgnored() throws SQLException {
//        flagHelper.addFlags("OracleSkipUncommittedRollbackOnlyTransactions");
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == COMMIT_XID) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 6L, endScn);
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1", "ROW_ID_5")));
        assertTrue(caughtRows.isEmpty());
    }

    @Test
    public void uncommittedTransactionNotContainingRollbacksNotIgnored() throws SQLException {
//        flagHelper.addFlags("OracleSkipUncommittedRollbackOnlyTransactions");
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3").build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == COMMIT_XID) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 2L, endScn);
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
//        assertThat(caughtRows, hasItem("001133"));
    }

    @Test
    public void noFlag_uncommittedTransactionContainingRollbacksNotIgnored() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, COMMIT_XID.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001133", "ROW_ID_2")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001133", "ROW_ID_3")
                                .addStatus(0)
                                .addRollback(1)
                                .build(),
                        new LogEntryBuilder(4L, START_XID.code, "001144", "ROW_ID_4").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001144", "ROW_ID_5").build());

        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == COMMIT_XID) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals("endScn", 2L, endScn);
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
//        assertThat(caughtRows, hasItem("001133"));
    }

    @Test
    public void writeCompletedTransactions() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(6L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1", "ROW_ID_2")));
    }

    @Test
    public void transactionWithDDL() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(5L, DDL_OP.code, "003344", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(6L, COMMIT_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        List<String> invalidTables = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                            invalidTables.add(invalid.name);
                        },
                        tableRef -> {
                        },
                        2L,
                        6L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(7L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
        assertThat(invalidTables, equalTo(ImmutableList.of("T")));
    }

    @Test
    public void writeEndingWithIncompleteTransaction() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(5L, INSERT_OP.code, "003344", "ROW_ID_3").build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(4L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
    }

    @Test
    public void writeStartingWithIncompleteTransaction() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(4L, COMMIT_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        4L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(1L));
        assertTrue(rowIds.isEmpty());
    }

    @Test
    public void writeIncompleteTransactionInMiddle() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, INSERT_OP.code, "003344", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "001122", "ROW_ID_2").build(),
                        new LogEntryBuilder(4L, COMMIT_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        4L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(3L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
    }

    @Test
    public void writeStartingWithRollbackTransaction() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, ROLLBACK_XID.code, "001122", null).build(),
                        new LogEntryBuilder(3L, START_XID.code, "003344", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                        },
                        tableRef -> {
                        },
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(6L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_2")));
    }

    @Test
    public void writeEndingWithRollbackTransaction() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(2L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(3L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(4L, ROLLBACK_XID.code, "003344", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {},
                        tableRef -> {},
                        1L,
                        4L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(5L));
        assertThat(rowIds, equalTo(ImmutableList.of("ROW_ID_1")));
    }

    @Test
    public void writeNoCompletedTransactions() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, START_XID.code, "003344", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {},
                        tableRef -> {},
                        1L,
                        4L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertThat(endScn, equalTo(1L));
        assertTrue(rowIds.isEmpty());
    }

    @Test
    public void logAllDDL() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, DDL_OP.code, "001122", null).build(),
                        new LogEntryBuilder(4L, DDL_OP.code, "001122", null).build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "001122", null).build());
        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> invalidated = new ArrayList<>();
        resultSet.rewind();
        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {
                    invalidated.add(invalid.name);
                },
                tableRef -> {},
                1L,
                5L,
                false,
                (op, tableRef, rowId) -> {});

        // 1 DML + 2 DDL
        assertEquals("Logged DDL Events", 3, invalidated.size());
    }

    @Test
    public void trueUncommittedTransactionShouldStopAtCurrentSCN_beginningWithUncommitted() throws SQLException {
//        flagHelper.addFlags("OracleProcessUncommittedTransactionsCommittedLater");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {},
                        tableRef -> {},
                        1L,
                        4L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals(endScn, 1L);
        assertTrue(rowIds.isEmpty());
    }

    @Test
    public void trueUncommittedTransactionShouldStopAtCurrentSCN_withUncommittedInMiddle() throws SQLException {
//        flagHelper.addFlags("OracleProcessUncommittedTransactionsCommittedLater");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(4L, INSERT_OP.code, "003344", "ROW_ID_2").build(),
                        new LogEntryBuilder(5L, INSERT_OP.code, "003344", "ROW_ID_3").build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        List<String> rowIds = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {},
                        tableRef -> {},
                        1L,
                        5L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                rowIds.add(rowId);
                            }
                        });

        assertEquals(endScn, 4L);
        assertEquals(rowIds, ImmutableList.of("ROW_ID_1"));
    }

    @Test
    public void uncommittedTaskAbove6Hr() throws SQLException {
//        flagHelper.addFlags("OracleResyncOn6HrUncommittedTxn");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        doAnswer(__ -> Instant.now().minus(Duration.ofHours(7))).when(api).convertScnToTimestamp(anyLong());

//        assertThrows(
//                CompleteWithTask.class,
//                () -> {
//                    oracleLogMinerIncrementalUpdater.writeCommittedRows(
//                            resultSet,
//                            () -> completed,
//                            SELECTED_TABLES,
//                            (invalid, reason) -> {},
//                            tableRef -> {},
//                            1L,
//                            3L,
//                            false,
//                            (op, schemaTable, rowId) -> {});
//                });
    }

    @Test
    public void uncommittedWarningAbove15Mins() throws SQLException {
//        flagHelper.addFlags("OracleResyncOn6HrUncommittedTxn");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        List<String> caughtRows = new ArrayList<>();

        api.setWarningIssuer(warning -> caughtRows.add(String.valueOf(((UncommittedWarning) warning).scn)));

        doAnswer(__ -> Instant.now().minus(Duration.ofMinutes(16))).when(api).convertScnToTimestamp(anyLong());

        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {},
                tableRef -> {},
                1L,
                3L,
                false,
                (op, schemaTable, rowId) -> {});

        assertEquals("Number of warning issued", 1, caughtRows.size());
    }

    @Test
    public void uncommittedWarningBelow15Mins_noWarning() throws SQLException {
//        flagHelper.addFlags("OracleResyncOn6HrUncommittedTxn");

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        List<String> caughtRows = new ArrayList<>();

        api.setWarningIssuer(warning -> caughtRows.add(String.valueOf(((UncommittedWarning) warning).scn)));

        doAnswer(__ -> Instant.now().minus(Duration.ofMinutes(5))).when(api).convertScnToTimestamp(anyLong());

        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {},
                tableRef -> {},
                1L,
                3L,
                false,
                (op, schemaTable, rowId) -> {});

        assertEquals("Number of warning issued", 0, caughtRows.size());
    }

    // Remove this method when OracleResyncOn6HrUncommittedTxn is rolled out
    @Test
    public void noFlag_uncommittedWarningWithinInterval() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(String.valueOf(((UncommittedWarning) warning).scn)));

        doAnswer(__ -> Instant.now().minus(Duration.ofHours(5))).when(api).convertScnToTimestamp(anyLong());

        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {},
                tableRef -> {},
                1L,
                3L,
                false,
                (op, schemaTable, rowId) -> {});

        assertEquals("Number of warning issued", 0, caughtRows.size());
    }

    @Test
    public void uncommittedWarning() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        doReturn(Instant.EPOCH).when(api).convertScnToTimestamp(anyLong());

        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {},
                tableRef -> {},
                1L,
                3L,
                false,
                (op, schemaTable, rowId) -> {});

        assertThat(caughtRows, hasItem("001122"));
    }

    @Test
    public void uncommittedWarning_whenMoreLogsExists() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").addStatus(1).build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001120", null).build());

        MockResultSet resultSet = new MockResultSet(rows);
        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);
        resultSet.rewind();

        List<String> caughtRows = new ArrayList<>();
        api.setWarningIssuer(warning -> caughtRows.add(((UncommittedWarning) warning).xid));

        oracleLogMinerIncrementalUpdater.writeCommittedRows(
                resultSet,
                () -> completed,
                SELECTED_TABLES,
                (invalid, reason) -> {},
                tableRef -> {},
                1L,
                3L,
                true,
                (op, schemaTable, rowId) -> {});

        assertThat(caughtRows, not(hasItem("001122")));
    }

    @Test
    public void processRowChunks_morePages_startGTECloseToEnd() {
        mockConnection();

        // need a really big page size so closeToEnd is more than a single page
        long trueEnd = 10000000;
        long closeToEnd = (long) (trueEnd * 0.9);

        SetOnce<Boolean> condition1Hit = new SetOnce<>();
        SetOnce<Boolean> condition2Hit = new SetOnce<>();

        Transactions.ChangedRowsResult res =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0,
                        trueEnd,
                        (conn, start, end, more) -> {
                            if (more) {
                                assertTrue("start is less than closeToEnd", start < closeToEnd);
                                condition1Hit.set(true);
                            } else {
                                assertTrue("start is greater than or equal to closeToEnd", start >= closeToEnd);
                                condition2Hit.set(true);
                            }
                            return end;
                        },
                        __ -> {},
                        Optional.of(api.pageSizeConfig.get().getStart()));

        assertTrue("\"start is less than closeToEnd\" condition hit", condition1Hit.isPresent());
        assertTrue("\"start is greater than or equal to closeToEnd\" condition hit", condition2Hit.isPresent());
        assertTrue("progress is greater than closeToEnd", res.earliestUncommittedScn >= closeToEnd);
    }

    @Test
    public void processRowChunks_morePages_endIsReached() {
        mockConnection();

        // need a smaller page size so closeToEnd is less than a single page
        long trueEnd = api.pageSizeConfig.get().getStart() * 3;

        SetOnce<Boolean> condition1Hit = new SetOnce<>();
        SetOnce<Boolean> condition2Hit = new SetOnce<>();

        Transactions.ChangedRowsResult res =
                oracleLogMinerIncrementalUpdater.processRowChunks(
                        0,
                        trueEnd,
                        (conn, start, end, more) -> {
                            if (more) {
                                assertTrue("end is less than trueEnd", end < trueEnd);
                                condition1Hit.set(true);
                            } else {
                                assertTrue("end is greater than or equal to trueEnd", end >= trueEnd);
                                condition2Hit.set(true);
                            }
                            return end;
                        },
                        __ -> {},
                        Optional.of(api.pageSizeConfig.get().getStart()));

        assertTrue("\"end is less than trueEnd\" condition hit", condition1Hit.isPresent());
        assertTrue("\"end is greater than or equal to trueEnd\" condition hit", condition2Hit.isPresent());
        assertEquals("progress is equal to end", trueEnd, res.earliestUncommittedScn);
    }

    @Test
    public void failOnSQLRecoverableException() throws SQLException {

        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        resultSet.rewind();
//        SQLRecoverableException exception =
//                assertThrows(
//                        SQLRecoverableException.class,
//                        () -> {
//                            oracleLogMinerIncrementalUpdater.writeCommittedRows(
//                                    resultSet,
//                                    () -> completed,
//                                    SELECTED_TABLES,
//                                    (invalid, reason) -> {},
//                                    tableRef -> {},
//                                    1L,
//                                    5L,
//                                    false,
//                                    (op, tableRef, rowId) -> {
//                                        throw new SQLRecoverableException("Fake Connection Reset");
//                                    });
//                        });
//        assertEquals(exception.getMessage(), "Fake Connection Reset");
    }

    @Test
    public void saveProgressOnSQLRecoverableException() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "001122", "ROW_ID_1").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "001122", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {},
                        tableRef -> {},
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {
                            if (op == INSERT_OP) {
                                throw new SQLRecoverableException("Fake Connection Reset");
                            }
                        });

        // we should have only processed the first 2 scns
        assertEquals("End SCN", 2L, endScn);
    }

    @Test
    public void writeCommittedRows_ignoreIgnorableDdl() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, DDL_OP.code, "1", null).addDdl("GRANT stuff;").build(),
                        new LogEntryBuilder(2L, START_XID.code, "2", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "2", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "2", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> invalidTables = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                            invalidTables.add(invalid.name);
                        },
                        tableRef -> {},
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {});

        assertEquals("endScn", 4L, endScn);
        assertThat(invalidTables, equalTo(ImmutableList.of()));
    }

    @Test
    public void writeCommittedRows_resyncNonIgnorableDdl() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, DDL_OP.code, "1", null)
                                .addDdl("ALTER TABLE make huge changes;")
                                .build(),
                        new LogEntryBuilder(2L, START_XID.code, "2", null).build(),
                        new LogEntryBuilder(2L, INSERT_OP.code, "2", "ROW_ID_2").build(),
                        new LogEntryBuilder(3L, COMMIT_XID.code, "2", null).build());
        MockResultSet resultSet = new MockResultSet(rows);

        Map<String, LogMinerOperation> completed = completedTransaction(resultSet);

        List<String> invalidTables = new ArrayList<>();
        resultSet.rewind();
        long endScn =
                oracleLogMinerIncrementalUpdater.writeCommittedRows(
                        resultSet,
                        () -> completed,
                        SELECTED_TABLES,
                        (invalid, reason) -> {
                            invalidTables.add(invalid.name);
                        },
                        tableRef -> {},
                        1L,
                        3L,
                        false,
                        (op, tableRef, rowId) -> {});

        assertEquals("endScn", 4L, endScn);
        assertThat(invalidTables, equalTo(ImmutableList.of("T")));
    }

}