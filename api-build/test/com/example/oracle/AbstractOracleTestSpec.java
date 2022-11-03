package com.example.oracle;

import com.example.core.TableRef;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/14/2021<br/>
 * Time: 6:44 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractOracleTestSpec {
    public DataSource dataSource;

    public static void mockConnection() {
        Connection mockConnection = mock(Connection.class);
        doReturn(mockConnection).when(ConnectionFactory.getInstance()).connectToSourceDb();
    }

    public static void mockConnection(OracleApi api) {
        Connection mockConnection = mock(Connection.class);
        doReturn(mockConnection).when(ConnectionFactory.getInstance()).connectToSourceDb();
    }

    public static Connection mockConnection(ResultSet resultSet) throws SQLException {
        Statement mockStatement = mock(Statement.class);
        doReturn(resultSet).when(mockStatement).executeQuery(anyString());
        return mockConnection(mockStatement);
    }

    public static Connection mockConnection(Statement mockStatement) throws SQLException {
        Connection mockConnection = mock(Connection.class);
        doReturn(mockStatement).when(mockConnection).createStatement();
        return mockConnection;
    }

    public OracleColumn makePkColumn(String columnName) {
        return new OracleColumn(
                columnName,
                OracleType.create("NUMBER"),
                true,
                new TableRef("test_schema", "test_name"),
                Optional.empty());
    }

    public static Connection mockSqlExceptionConnection(String message) throws SQLException {
        Statement mockStatement = mock(Statement.class);
        doThrow(new SQLException(message)).when(mockStatement).executeQuery(anyString());
        return mockConnection(mockStatement);
    }

    public static Answer<Boolean> answerForMaxLogminerDuration(int upto) {
        AtomicInteger answerCount = new AtomicInteger();
        return i -> {
            if (answerCount.getAndIncrement() <= upto) return true;
            throw new SQLException("...ORA-01291: missing log file...");
        };
    }

    public Map<String, List<String>> createTestSet(int schemaCount, int tableCount) {
        Map<String, List<String>> schemas = new HashMap<>();
        for (int i = 0; i < schemaCount; i++) {
            List<String> tables = new ArrayList<>();
            for (int t = 0; t < tableCount; t++) {
                tables.add("T" + t);
            }
            schemas.put("S" + i, tables);
        }
        return schemas;
    }

    public Connection getMockConnectionForAlterSessionTests(ResultSet resultSet, String returnString)
            throws Exception {
        Connection mockConnection = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        doReturn(mockConnection).when(dataSource).getConnection();
        doReturn(mockStatement).when(mockConnection).createStatement();
        doReturn(true).when(resultSet).next();
        doReturn(returnString).when(resultSet).getString(anyString());
        doReturn(resultSet).when(mockStatement).executeQuery(any());
        return mockConnection;
    }
}