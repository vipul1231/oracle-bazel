package com.example.snowflake;

import com.example.snowflake.util.DBUtility;
import com.snowflake.client.jdbc.SnowflakeDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class SnowflakeClientSpec {

    SnowflakeDriver snowflakeDriver = new SnowflakeDriver();
    Connection con = null;

    @Before
    public void before() {
        try {
            con = snowflakeDriver.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testCreateTable() {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("create or replace table test(id STRING, name STRING)");
            stmt.executeUpdate("insert into test values (1, 'hello world')");

            rs = stmt.executeQuery("SELECT * FROM test");
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount();
                 colIdx++) {
                System.out.println("Column " + colIdx + ": type=" +
                        resultSetMetaData.getColumnTypeName(colIdx + 1));
            }

            int count = 0;
            int rowIdx = 0;
            while (rs.next()) {
                System.out.println("row " + rowIdx + ", column 0: " +
                        rs.getString(1));
                count++;
            }

            Assert.assertEquals(count, 1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBUtility.close(stmt);
            DBUtility.close(rs);
        }
    }

    @After
    public void after() {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("drop table test");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBUtility.close(stmt);
        }

        // close the connection
        DBUtility.close(con);
    }
}
