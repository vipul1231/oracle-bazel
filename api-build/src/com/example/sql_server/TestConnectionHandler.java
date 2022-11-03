package com.example.sql_server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/5/2021<br/>
 * Time: 8:39 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class TestConnectionHandler {
    private static final String url = "jdbc:sqlserver://localhost:1433;databaseName=Demo;user=thilanka;password=d1leepa32i";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }
}