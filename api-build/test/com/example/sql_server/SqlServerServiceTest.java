package com.example.sql_server;

import com.example.core.TableRef;
import com.example.core2.Output;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 10:22 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class SqlServerServiceTest {


    private static final String UPDATER = "INCREMENTAL_UPDATER";
    private static final String IMPORTER = "IMPORTER";

    public static final String TEST_SCHEME = "dbo";
    static Set<TableRef> includedTables = new HashSet<>();

    private static List<Operation> operations = new ArrayList<>();

    class Operation {
        public Operation(String tableName, String source, LocalDateTime time) {
            this.tableName = tableName;
            this.source = source;
            this.time = time;
        }

        public Operation() {
        }

        private String tableName;
        private String source;
        private LocalDateTime time;

        public String getTableName() {
            return tableName;
        }

        public String getSource() {
            return source;
        }

        public LocalDateTime getTime() {
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
    public void prePopulate() {
        includedTables.add(new TableRef(TEST_SCHEME, "tbl_test_1"));
        includedTables.add(new TableRef(TEST_SCHEME, "tbl_test_2"));
        includedTables.add(new TableRef(TEST_SCHEME, "tbl_test_3"));
//        includedTables.add(new TableRef(TEST_SCHEME, "tbl_test_4"));
    }

    private static void startRandomUpdateJob() {
        Runnable randomUpdater = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 20; i++) {
                    System.out.println("---------------------------------- Random Update ----------------");
                    DBUtil.updateTable("tbl_test_1", "r");
                    DBUtil.updateTable("tbl_test_2", "r");
                    DBUtil.updateTable("tbl_test_3", "r");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        new Thread(randomUpdater).start();
    }

    @Test
    public void testOutOfOrderIssue() throws Exception {
        //
        System.out.println("#################### testOutOfOrderIssue ##################");

        // start a new thread, which will do random updates to tables
        startRandomUpdateJob();
        // -------------------------------------------------------

        SqlServerInformer mockInformer = mock(SqlServerInformer.class);

        SqlServerService sqlServerService = new SqlServerService();
        SqlServerService mockSqlServer = Mockito.spy(sqlServerService);

        SqlServerSource mockSqlServerSource = Mockito.mock(SqlServerSource.class);
        doAnswer(new Answer<Connection>() {
            @Override
            public Connection answer(InvocationOnMock invocationOnMock) throws Throwable {
                return TestConnectionHandler.getConnection();
            }
        }).when(mockSqlServerSource).connection();


        SqlServerState mockSqlServerState = Mockito.spy(SqlServerState.class);
//        Mockito.doNothing().when(mockSqlServerState).setImportFinishedV3(any());
        Output mockOutput = Mockito.spy(Output.class);

        when(mockInformer.version()).thenReturn(Optional.empty());
        doAnswer(new Answer<List<TableRef>>() {
            @Override
            public List<TableRef> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new ArrayList<>(invocationOnMock.getArgument(0));
            }
        }).when(mockInformer).filterForChangeType(any(), any());


        when(mockInformer.includedTables()).thenReturn(includedTables);
        when(mockInformer.excludedTables()).thenReturn(new HashSet<>());

        doAnswer(new Answer<SqlServerTableInfo>() {
            @Override
            public SqlServerTableInfo answer(InvocationOnMock invocationOnMock) throws Throwable {
                TableRef sourceTable = invocationOnMock.getArgument(0);
                String tableWithSchema = sourceTable.getSchema() + "_" + sourceTable.getName();

                List<SqlServerColumnInfo> sourceColumnInfo = new ArrayList<>();
                sourceColumnInfo.add(new SqlServerColumnInfo(sourceTable, "visit_id", 0, 0, null, null, null, null, null, false, false, false, false, null, false, false));
                SqlServerTableInfo sqlServerTableInfo = new SqlServerTableInfo(((TableRef) invocationOnMock.getArgument(0)), null, -1, -1, sourceColumnInfo, -1, new HashSet<>(), SqlServerChangeType.NONE, tableWithSchema, false, null, null);
                return sqlServerTableInfo;
            }
        }).when(mockInformer).tableInfo(any());

        SqlServerImporter mockSqlServerImporter = Mockito.mock(SqlServerImporter.class, withSettings().useConstructor(mockSqlServerSource, mockSqlServerState, mockInformer, mockOutput));
        // mock import finished method
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
//                System.out.println("mockSqlServerImporter :: importFinished : - " + invocationOnMock.getArgument(0));
                TableRef sourceTable = invocationOnMock.getArgument(0);

                boolean importFinished = false;
                for (Operation operation : operations) {
                    if (operation.getSource().equals(IMPORTER) && operation.getTableName().equals(sourceTable.name)) {
                        importFinished = true;
                        break;
                    }
                }

                return importFinished;
            }
        }).when(mockSqlServerImporter).importFinished(any());
        Mockito.doReturn(true).when(mockSqlServerImporter).importStarted(Mockito.any(TableRef.class));

        SqlServerIncrementalUpdater mockIncrementalUpdater = Mockito.spy(new SqlServerIncrementalUpdater(mockSqlServerSource, mockSqlServerState, mockInformer, mockOutput));
        doCallRealMethod().when(mockIncrementalUpdater).incrementalUpdate(any());
        doNothing().when(mockIncrementalUpdater).incrementalUpdateCTTables(any());

        SqlServerIncrementalUpdater.DataSynchronizationStrategy mockDataSynchronizationStrategy = Mockito.mock(SqlServerIncrementalUpdater.HistoryModeSyncStrategy.class);
        when(mockIncrementalUpdater.getSyncStrategy(anyBoolean(), any(), any(), any(), any())).thenReturn(mockDataSynchronizationStrategy);
        doAnswer(new Answer<Optional>() {
            @Override
            public Optional answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("SqlServerIncrementalUpdater :: cdcCatchUp : - " + invocationOnMock.getArgument(1));

                TableRef sourceTable = ((SqlServerTableInfo) invocationOnMock.getArgument(1)).sourceTable;
                int dummySleep = (Integer.parseInt(sourceTable.name.split("_")[2]) * 100);
                Thread.sleep(dummySleep);

                DBUtil.updateTable(sourceTable.name, "u");
                operations.add(new Operation(sourceTable.name, UPDATER, LocalDateTime.ofInstant(invocationOnMock.getArgument(2), ZoneId.systemDefault())));

                return Optional.empty();
            }
        }).when(mockDataSynchronizationStrategy).sync(any());


        // mock procssRow method
        doAnswer(new Answer<Optional>() {
            @Override
            public Optional answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println("mockSqlServerImporter :: processRows : - " + invocationOnMock.getArgument(1));

                TableRef sourceTable = ((SqlServerTableInfo) invocationOnMock.getArgument(1)).sourceTable;
                int dummySleep = (Integer.parseInt(sourceTable.name.split("_")[2]) * 1000);
                Thread.sleep(dummySleep);

                // dummy update to the table
                DBUtil.updateTable(sourceTable.name, "i");

                operations.add(new Operation(sourceTable.name, IMPORTER, LocalDateTime.now()));

                return Optional.empty();
            }
        }).when(mockSqlServerImporter).processRows(any(), any(), any());
        doCallRealMethod().when(mockSqlServerImporter).importPage(any());

        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.doReturn(mockPreparedStatement).when(mockSqlServerImporter).selectRows(any(), any(), any());
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.doReturn(mockResultSet).when(mockPreparedStatement).executeQuery();

        when(mockSqlServer.informer(any(), any())).thenReturn(mockInformer);
        when(mockSqlServer.importer(any(), any(), any(), any())).thenReturn(mockSqlServerImporter);
        when(mockSqlServer.incrementalUpdater(any(), any(), any(), any())).thenReturn(mockIncrementalUpdater);

        try {
            mockSqlServer.update(null, mockSqlServerState, null, new Output<>(), null, null);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail(exception.getMessage());
        }

        System.out.println("###################### Start Operations ####################");
        operations.forEach(operation -> {
            System.out.println(operation);
        });
        System.out.println("###################### End Operations ####################");

        System.out.println("Asserting that importShouldHappenBeforeUpdate for each table");
        Assert.assertTrue("Update happened before import for tbl_test_1", importShouldHappenBeforeUpdate("tbl_test_1"));
        Assert.assertTrue("Update happened before import for tbl_test_2", importShouldHappenBeforeUpdate("tbl_test_2"));
        Assert.assertTrue("Update happened before import for tbl_test_3", importShouldHappenBeforeUpdate("tbl_test_3"));
        Assert.assertTrue("Update happened before import for tbl_test_4", importShouldHappenBeforeUpdate("tbl_test_4"));

        System.out.println("----------------------------- Process Completed --------------------------------");
    }

    public boolean importShouldHappenBeforeUpdate(String tableName) {
        // sort by time
        List<Operation> operationsPerTable = operations.stream().filter(operation -> operation.getTableName().equals(tableName))
                .sorted(Comparator.comparing(Operation::getTime))
                .collect(Collectors.toList());

        if (operationsPerTable.size() > 0) {
            return (operationsPerTable.get(0).getSource().equals(IMPORTER));
        }
        return true;
    }
}