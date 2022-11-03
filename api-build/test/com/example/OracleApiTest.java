package com.example;

import com.example.core.TableRef;
import com.example.oracle.LogMinerOperation;
import com.example.oracle.OracleApi;
import com.example.oracle.logminer.LogMinerContentsDao;
import com.example.oracle.logminer.LogMinerSession;
import com.example.oracle.logminer.LogMinerSetupValidator;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/1/2021<br/>
 * Time: 5:37 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleApiTest {

    final static String DB_URL = "jdbc:oracle:thin:@db202106301653_medium?TNS_ADMIN=wallet/";

    final static String DB_USER = "ADMIN";
    final static String DB_PASSWORD = "D1leepa32i_139";

    @Test
    public void testConnectionFactory() {
//        method().when
    }

    public void method() {

    }


    @Test
    public void testDbConnection() {
        System.out.println("################### Test DB Connection ###################");

        Properties info = new Properties();
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");


        OracleDataSource ods = null;
        try {
            ods = new OracleDataSource();
            ods.setURL(DB_URL);
            ods.setConnectionProperties(info);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail(throwables.getMessage());
        }

        OracleApi oracleApi = new OracleApi(ods);
        System.out.println("Before Creating the connection from OracleApi");
        try {
            // create the connection to oracle api
            Connection connection = oracleApi.connectToSourceDb();
            System.out.println("Connection Created Successfully");
            Map<TableRef, Optional<String>> tableMap = oracleApi.tables();
            Assert.assertNotNull(tableMap);
            System.out.println("tableMap :" + tableMap.toString());
            Assert.assertTrue(!tableMap.isEmpty());

            System.out.println("################### Test DB Data Retrieval Successful ###################");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        System.out.println("################### Process Completed ###################");
    }

    @Test
    public void testLogminer() {
        System.out.println("################### Test Logminer Connection ###################");

        Properties info = new Properties();
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");


        OracleDataSource ods = null;
        try {
            ods = new OracleDataSource();
            ods.setURL(DB_URL);
            ods.setConnectionProperties(info);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail(throwables.getMessage());
        }

        OracleApi oracleApi = new OracleApi(ods);
        System.out.println("Before Creating the connection from OracleApi");
        try {
            // create the connection to oracle api
            Connection connection = oracleApi.connectToSourceDb();
            System.out.println("Connection Created Successfully");

            LogMinerSetupValidator.validate(new LogMinerSession(connection));
            LogMinerContentsDao logMinerContentsDao = new LogMinerContentsDao(connection);
            Map<String, LogMinerOperation> logMinerOperationMap = logMinerContentsDao.completedTransactions(connection);
            System.out.println("logMinerOperationMap :" + logMinerOperationMap);

            System.out.println("################### Test DB Data Retrieval Successful ###################");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        System.out.println("################### Process Completed ###################");
    }
}