package com.example.sql_server;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/4/2021<br/>
 * Time: 2:50 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionTest {
    String url = "jdbc:sqlserver://localhost:1433;databaseName=Demo;user=thilanka;password=d1leepa32i";

    public static int getRandomNumber() {
        Random r = new Random();
        int low = 1;
        int high = 100;
        return r.nextInt(high - low) + low;
    }

    @Test
    public void testUpdate() {
        String sql = "UPDATE tbl_test_1 SET first_name=? WHERE visit_id=?";
        int count = 1;
        try (Connection connection = DriverManager.getConnection(url); PreparedStatement statement = connection.prepareStatement(sql);) {
            statement.setString(count++, "Edited-" + getRandomNumber());
            statement.setInt(count++, 1);

            int rowsUpdated = statement.executeUpdate();
            Assert.assertTrue(rowsUpdated > 0);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testConnection() {
        ResultSet resultSet;
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();) {

            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT * from test";
            resultSet = statement.executeQuery(selectSql);

            Assert.assertNotNull(resultSet);
            // Print results from select statement
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}