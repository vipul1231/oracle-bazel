package com.example.oracle;

import com.example.db.MockResultSet;
import com.example.oracle.logminer.LogMinerContentsDao;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.oracle.LogMinerOperation.COMMIT_XID;
import static com.example.oracle.LogMinerOperation.START_XID;
//import static org.hamcrest.Matchers.containsInAnyOrder;
//import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/14/2021<br/>
 * Time: 7:38 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class LogMinerContentsDaoSpec {

    private static final LogMinerOperation ROLLBACK_XID = null;
    private LogMinerContentsDao logMinerContentsDao;

    @Before
    public void beforeLogMinerContentsDaoSpec() {
        logMinerContentsDao = mock(LogMinerContentsDao.class);
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
    public void completedTransactionWithCommit() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, COMMIT_XID.code, "001122", null).build());

        Map<String, LogMinerOperation> completed = completedTransaction(new MockResultSet(rows));

//        assertThat(completed.keySet(), equalTo(ImmutableSet.of("001122")));
    }

    @Test
    public void completedTransactionWithRollback() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, ROLLBACK_XID.code, "001122", null).build());

        Map<String, LogMinerOperation> completed = completedTransaction(new MockResultSet(rows));

//        assertThat(completed.keySet(), equalTo(ImmutableSet.of("001122")));
    }

    @Test
    public void incompleteTransaction() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(2L, COMMIT_XID.code, "001122", null).build(),
                        new LogEntryBuilder(3L, START_XID.code, "003344", null).build());

        Map<String, LogMinerOperation> completed = completedTransaction(new MockResultSet(rows));

//        assertThat(completed.keySet(), equalTo(ImmutableSet.of("001122")));
    }

    @Test
    public void noCompletedTransactions() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "001122", null).build(),
                        new LogEntryBuilder(3L, START_XID.code, "003344", null).build());

        Map<String, LogMinerOperation> completed = completedTransaction(new MockResultSet(rows));

        assertTrue(completed.keySet().isEmpty());
    }

    @Test
    public void interleavedTransactions() throws SQLException {
        List<Map<String, Object>> rows =
                Arrays.asList(
                        new LogEntryBuilder(1L, START_XID.code, "13001F00431F1A00", null).build(),
                        new LogEntryBuilder(4L, START_XID.code, "130019009D211A00", null).build(),
                        new LogEntryBuilder(5L, COMMIT_XID.code, "130019009D211A00", null).build(),
                        new LogEntryBuilder(8L, START_XID.code, "0D000B00C6BD0600", null).build(),
                        new LogEntryBuilder(8L, START_XID.code, "05000F0087880700", null).build(),
                        new LogEntryBuilder(8L, COMMIT_XID.code, "0D000B00C6BD0600", null).build(),
                        new LogEntryBuilder(8L, START_XID.code, "0F00170076960700", null).build(),
                        new LogEntryBuilder(8L, START_XID.code, "0A001C00302D0700", null).build(),
                        new LogEntryBuilder(8L, COMMIT_XID.code, "13001F00431F1A00", null).build(),
                        new LogEntryBuilder(8L, COMMIT_XID.code, "0F00170076960700", null).build());

        Map<String, LogMinerOperation> completed = completedTransaction(new MockResultSet(rows));

//        assertThat(
//                completed.keySet(),
//                containsInAnyOrder("130019009D211A00", "0D000B00C6BD0600", "0F00170076960700", "13001F00431F1A00"));
    }
}