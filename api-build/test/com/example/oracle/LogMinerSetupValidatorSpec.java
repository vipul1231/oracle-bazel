package com.example.oracle;

import com.example.oracle.logminer.LogMinerSetupValidator;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/15/2021<br/>
 * Time: 7:50 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class LogMinerSetupValidatorSpec extends AbstractOracleTestSpec {

    private LogMinerSetupValidator logMinerSetupValidator = null;

    @Before
    public void beforeLogMinerSetupValidatorSpec() {
        logMinerSetupValidator = mock(LogMinerSetupValidator.class);

        dataSource = mock(DataSource.class);
    }

    @Test
    public void maxLogminerDuration_ShouldBe5() throws SQLException {
        // Arrange
        PreparedStatement mockStatement = mock(PreparedStatement.class);
//        ResultSet mockResultSet =
//                resultSetFrom(
//                        columns(
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_TIME,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_TIME),
//                        rows(
//                                row(
//                                        Long.valueOf(10000L),
//                                        Long.valueOf(11000L),
//                                        timestamp("2021-01-01 01:00:00.000"),
//                                        timestamp("2021-01-01 06:00:00.000"))));

//        doReturn(mockResultSet).when(mockStatement).executeQuery();

        Connection mockConnection = mock(Connection.class);
        doReturn(mockStatement).when(mockConnection).prepareStatement(anyString());

//        doReturn(mockConnection).when(dataSource).getConnection();

        // Act
        int maxLogminerDuration = logMinerSetupValidator.getMaxLogminerDuration();

        // Assert
        assertEquals(5, maxLogminerDuration);
    }

    @Test
    public void test_alterSessionShouldExecuteForMaxLogminerDuration() throws Exception {
        // prepare
//        flagHelper.addFlags("OraclePDB");
        Connection mockConnection = mock(Connection.class);
        doReturn(mockConnection).when(dataSource).getConnection();

        Statement mockStatement = mock(Statement.class);
        doReturn(mockStatement).when(mockConnection).createStatement();

        ResultSet mockResultSet1 = mock(ResultSet.class);
        doReturn(mockResultSet1).when(mockStatement).executeQuery(any());
        doReturn(true).when(mockResultSet1).next();
        doReturn("YES").when(mockResultSet1).getString(anyString());

        PreparedStatement mockPreparedStmt = mock(PreparedStatement.class);
        doReturn(mockPreparedStmt).when(mockConnection).prepareStatement(anyString());
//        ResultSet mockResultSet =
//                resultSetFrom(
//                        columns(
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_TIME,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_TIME),
//                        rows(
//                                row(
//                                        Long.valueOf(10000L),
//                                        Long.valueOf(11000L),
//                                        timestamp("2021-01-01 01:00:00.000"),
//                                        timestamp("2021-01-01 06:00:00.000"))));

//        doReturn(mockResultSet).when(mockPreparedStmt).executeQuery();

        //
//        api.connectionManagerWithRetry = spy(new ConnectionManagerWithRetry(dataSource, Optional.of("PDB")));

        // execute
        int maxLogminerDuration = logMinerSetupValidator.getMaxLogminerDuration();

        // Assert
        assertEquals(5, maxLogminerDuration);
//        verify(api.connectionManagerWithRetry, times(1)).alterConnectionSession(any(), anyBoolean());
    }


    @Test
    public void maxLogminerDuration_ShouldBe18() throws SQLException {
        // Arrange
        PreparedStatement mockStatement = mock(PreparedStatement.class);
//        ResultSet mockResultSet =
//                resultSetFrom(
//                        columns(
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_CHANGE_NUM,
//                                ArchivedLogDerivedData.ColumnDef.MIN_FIRST_TIME,
//                                ArchivedLogDerivedData.ColumnDef.MAX_NEXT_TIME),
//                        rows(
//                                row(
//                                        Long.valueOf(10000L),
//                                        Long.valueOf(11000L),
//                                        timestamp("2021-01-01 01:00:00.000"),
//                                        timestamp("2021-01-01 19:00:00.000"))));

//        doReturn(mockResultSet).when(mockStatement).executeQuery();

        Connection mockConnection = mock(Connection.class);
        doReturn(mockStatement).when(mockConnection).prepareStatement(anyString());

        doReturn(mockConnection).when(dataSource).getConnection();

        // Act
        int maxLogminerDuration = logMinerSetupValidator.getMaxLogminerDuration();

        // Assert
        assertEquals(18, maxLogminerDuration);
    }
}