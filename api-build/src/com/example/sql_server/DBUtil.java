package com.example.sql_server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/7/2021<br/>
 * Time: 11:51 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class DBUtil {
    public static final String url = "jdbc:sqlserver://localhost:1433;databaseName=Demo;user=thilanka;password=d1leepa32i";

    /**
     * DO a random update to a table
     *
     * @param table
     */
    public static void updateTable(String table, String source) {
        String sql = "UPDATE " + table + " SET first_name=? WHERE visit_id=?";
        int count = 1;
        try (Connection connection = DriverManager.getConnection(url); PreparedStatement statement = connection.prepareStatement(sql);) {
            statement.setString(count++, "Edited-" + source + "-" + getRandomNumber());
            statement.setInt(count++, 1);

            int rowsUpdated = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static int getRandomNumber() {
        Random r = new Random();
        int low = 1;
        int high = 100;
        return r.nextInt(high - low) + low;
    }
}