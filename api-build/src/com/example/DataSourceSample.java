package com.example;

import com.example.core.TableRef;
import com.example.oracle.OracleApi;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

import java.sql.*;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class DataSourceSample {
    final static String DB_URL = "jdbc:oracle:thin:@db202106301653_medium?TNS_ADMIN=wallet/";

    final static String DB_USER = "ADMIN";
    final static String DB_PASSWORD = "D1leepa32i_139";

    /*
     * The method gets a database connection using
     * oracle.jdbc.pool.OracleDataSource. It also sets some connection
     * level properties, such as,
     * OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH,
     * OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES, etc.,
     * There are many other connection related properties. Refer to
     * the OracleConnection interface to find more.
     */
    public static void main(String args[]) throws SQLException {
        Properties info = new Properties();
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");


        OracleDataSource ods = new OracleDataSource();
        ods.setURL(DB_URL);
        ods.setConnectionProperties(info);

        OracleApi oracleApi = new OracleApi(ods);
        System.out.println("Before Creating the connection from OracleApi");
        try {
            Connection connection = oracleApi.connectToSourceDb();
            System.out.println("Connection Created Successfully");
            Map<TableRef, Optional<String>> tableMap = oracleApi.tables();
            System.out.println("tableMap :" + tableMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //        connectAndPrint(ods);
    }

    public static void connectAndPrint(OracleDataSource ods) throws SQLException {
        // With AutoCloseable, the connection is closed automatically.
        try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
            // Get the JDBC driver name and version
            DatabaseMetaData dbmd = connection.getMetaData();
            System.out.println("Driver Name: " + dbmd.getDriverName());
            System.out.println("Driver Version: " + dbmd.getDriverVersion());
            // Print some connection properties
            System.out.println("Default Row Prefetch Value is: " +
                    connection.getDefaultRowPrefetch());
            System.out.println("Database Username is: " + connection.getUserName());
            System.out.println();
            // Perform a database operation
            printDepartments(connection);
        }
    }

    /*
     * Displays first_name and last_name from the employees table.
     */
    public static void printDepartments(Connection connection) throws SQLException {
        // Statement and ResultSet are AutoCloseable and closed automatically.
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement
                    .executeQuery("select name, location from departments")) {
                System.out.println("NAME" + "  " + "LOCATION");
                System.out.println("---------------------");
                while (resultSet.next())
                    System.out.println(resultSet.getString(1) + " "
                            + resultSet.getString(2) + " ");
            }
        }
    }
}