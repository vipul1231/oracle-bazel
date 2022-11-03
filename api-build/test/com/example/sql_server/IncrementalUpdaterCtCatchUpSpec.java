package com.example.sql_server;

import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.Output;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test case to demonstrate that incremental updater will never happen before importer has started
 */
public class IncrementalUpdaterCtCatchUpSpec {

    private static final String UPDATER = "INCREMENTAL_UPDATER";
    private static final String IMPORTER = "IMPORTER";
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

    SqlServerSource mockSqlServerSource;
    SqlServerState mockSqlServerState;
    SqlServerInformer mockInformer;
    Output mockOutput;
    Connection mockConnection;

    @Before
    public void prepare() {
        mockSqlServerSource = mock(SqlServerSource.class);
        mockSqlServerState = Mockito.spy(SqlServerState.class);
        mockInformer = mock(SqlServerInformer.class);
        mockOutput = Mockito.spy(Output.class);
        mockConnection = mock(Connection.class);
    }

    @Test
    public void testUpdaterSkipIfImporterNotStarted() throws SQLException {
        operations.clear();
        SqlServerIncrementalUpdater mockIncrementalUpdater = Mockito.spy(new SqlServerIncrementalUpdater(mockSqlServerSource, mockSqlServerState, mockInformer, mockOutput));

        Instant importerStartTime = Instant.now();
        Instant updaterStartTime = importerStartTime.plus(10, ChronoUnit.SECONDS);

        TableRef tableRef = new TableRef("dbo", "test");

        mockSqlServerState.setCheckWithImportStartTime(true);
        // Importer is not started

        when(mockInformer.tableInfo(any())).thenReturn(new SqlServerTableInfo(tableRef, null, -1, -1, -1));
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.doReturn(mockResultSet).when(mockPreparedStatement).executeQuery();

        when(mockInformer.syncModes()).thenAnswer(invocationOnMock -> {
            Map<TableRef, SyncMode> map = new HashMap<>();
            map.put(tableRef, SyncMode.History);
            return map;
        });

        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockResultSet.next()).thenAnswer(invocation -> {
            return operations.isEmpty();
        });
        when(mockResultSet.getString("SYS_CHANGE_OPERATION")).thenReturn("U");
        when(mockResultSet.getTimestamp(any())).thenReturn(Timestamp.from(updaterStartTime));

        doReturn(new HashMap()).when(mockIncrementalUpdater).extractRowValues(any(), any(), any(), anyBoolean());
        doNothing().when(mockIncrementalUpdater).deleteRecord(any(), any(), any());
        doAnswer(call -> {
            operations.add(new Operation(tableRef.name, UPDATER, call.getArgument(2)));
            return null;
        }).when(mockIncrementalUpdater).upsertRecord(any(), any(), any());

        try {
            mockIncrementalUpdater.ctCatchUp(mockConnection, tableRef, 1L);
            Assert.assertTrue("Importer not started for table-test, so updater will be skipped", operations.isEmpty());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Use import begin time, not finish time, to discard update record before import start
     * Reset operation time to import begin time, when import has NOT been finished
     *
     * @throws SQLException
     */
    @Test
    public void testUpdaterProceedIfImporterStarted() throws SQLException {
        operations.clear();
        SqlServerIncrementalUpdater mockIncrementalUpdater = Mockito.spy(new SqlServerIncrementalUpdater(mockSqlServerSource, mockSqlServerState, mockInformer, mockOutput));

        Instant importerStartTime = Instant.now();
        Instant updaterStartTime = importerStartTime.plus(10, ChronoUnit.SECONDS);

        TableRef tableRef = new TableRef("dbo", "test");

        mockSqlServerState.setCheckWithImportStartTime(true);
        // mark importer as started
        mockSqlServerState.initTableState(tableRef, importerStartTime);

        when(mockInformer.tableInfo(any())).thenReturn(new SqlServerTableInfo(tableRef, null, -1, -1, -1));
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.doReturn(mockResultSet).when(mockPreparedStatement).executeQuery();

        when(mockInformer.syncModes()).thenAnswer(invocationOnMock -> {
            Map<TableRef, SyncMode> map = new HashMap<>();
            map.put(tableRef, SyncMode.History);
            return map;
        });
        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockResultSet.next()).thenAnswer(invocation -> {
            return operations.isEmpty();
        });
        when(mockResultSet.getString("SYS_CHANGE_OPERATION")).thenReturn("U");
        when(mockResultSet.getTimestamp(any())).thenReturn(Timestamp.from(updaterStartTime));

        doReturn(new HashMap()).when(mockIncrementalUpdater).extractRowValues(any(), any(), any(), anyBoolean());
        doNothing().when(mockIncrementalUpdater).deleteRecord(any(), any(), any());
        doAnswer(call -> {
            operations.add(new Operation(tableRef.name, UPDATER, call.getArgument(2)));
            return null;
        }).when(mockIncrementalUpdater).upsertRecord(any(), any(), any());

        try {
            mockIncrementalUpdater.ctCatchUp(mockConnection, tableRef, 1L);
            // Use import begin time, not finish time, to discard update record before import start
            Assert.assertTrue("Use import begin time, not finish time, to discard update record before import start", !operations.isEmpty());

            // Reset operation time to import begin time, when import has NOT been finished
            Assert.assertTrue("Reset operation time to import begin time, when import has NOT been finished", operations.get(0).getTime().equals(importerStartTime));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }


    /**
     * if import is finished, we dont update the update time
     * @throws SQLException
     */
    @Test
    public void testNotUpdateTimeIfImportIsFinished() throws SQLException {
        operations.clear();
        SqlServerIncrementalUpdater mockIncrementalUpdater = Mockito.spy(new SqlServerIncrementalUpdater(mockSqlServerSource, mockSqlServerState, mockInformer, mockOutput));

        Instant importerStartTime = Instant.now();
        Instant updaterStartTime = importerStartTime.plus(10, ChronoUnit.SECONDS);

        TableRef tableRef = new TableRef("dbo", "test");

        mockSqlServerState.setCheckWithImportStartTime(true);
        // mark importer as started
        mockSqlServerState.initTableState(tableRef, importerStartTime);
        // mark import as finished
        mockSqlServerState.setImportFinishedV3(tableRef);

        when(mockInformer.tableInfo(any())).thenReturn(new SqlServerTableInfo(tableRef, null, -1, -1, -1));
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.doReturn(mockResultSet).when(mockPreparedStatement).executeQuery();

        when(mockInformer.syncModes()).thenAnswer(invocationOnMock -> {
            Map<TableRef, SyncMode> map = new HashMap<>();
            map.put(tableRef, SyncMode.History);
            return map;
        });
        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockResultSet.next()).thenAnswer(invocation -> {
            return operations.isEmpty();
        });
        when(mockResultSet.getString("SYS_CHANGE_OPERATION")).thenReturn("U");
        when(mockResultSet.getTimestamp(any())).thenReturn(Timestamp.from(updaterStartTime));

        doReturn(new HashMap()).when(mockIncrementalUpdater).extractRowValues(any(), any(), any(), anyBoolean());
        doNothing().when(mockIncrementalUpdater).deleteRecord(any(), any(), any());
        doAnswer(call -> {
            operations.add(new Operation(tableRef.name, UPDATER, call.getArgument(2)));
            return null;
        }).when(mockIncrementalUpdater).upsertRecord(any(), any(), any());

        try {
            mockIncrementalUpdater.ctCatchUp(mockConnection, tableRef, 1L);
            // Use import begin time, not finish time, to discard update record before import start
            Assert.assertTrue("Use import begin time, not finish time, to discard update record before import start", !operations.isEmpty());

            // Import has finished, so don't reset the upsert time
            Assert.assertTrue("Import has finished, so don't reset the upsert time", operations.get(0).getTime().isAfter(importerStartTime));
            // Import has finished, so don't reset the upsert time. So its the updaterStartTime
            Assert.assertTrue("Import has finished, so don't reset the upsert time. So its the updaterStartTime", operations.get(0).getTime().equals(updaterStartTime));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
