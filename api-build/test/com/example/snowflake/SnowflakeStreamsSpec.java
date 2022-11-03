package com.example.snowflake;

import com.example.snowflake.util.DBUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SnowflakeStreamsSpec {

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
    public void testStreams() {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("create or replace table user(id STRING, name STRING)");
            stmt.executeUpdate("insert into user values (1, 'test_user')");

            // create the stream
            stmt.executeUpdate("create or replace stream user_stream on table user append_only = false show_initial_rows = true");

            rs = stmt.executeQuery("SELECT * FROM user");
            Assert.assertTrue(rs.next());

            // check whether we have that in the stream
            rs = stmt.executeQuery("SELECT SYSTEM$STREAM_HAS_DATA('user_stream')");
            Assert.assertTrue(rs.next());

            rs = stmt.executeQuery("SELECT * from user_stream");
            while (rs.next()) {
                System.out.println(rs.getString("NAME") + " = " + rs.getString("METADATA$ACTION"));
                Assert.assertEquals(rs.getString("NAME"), "test_user");
                Assert.assertEquals(rs.getString("METADATA$ACTION"), "INSERT");
            }

            // do an update
            stmt.executeUpdate("update user set name='test_user_updated' where ID = '1'");
            rs = stmt.executeQuery("SELECT * from user");
            while (rs.next()) {
                System.out.println(rs.getString("NAME"));
            }

            // select a stream wont update the offset
            rs = stmt.executeQuery("SELECT * from user_stream");
            while (rs.next()) {
                System.out.println(rs.getString("NAME") + " = " + rs.getString("METADATA$ACTION"));
                Assert.assertEquals(rs.getString("NAME"), "test_user");
                Assert.assertEquals(rs.getString("METADATA$ACTION"), "INSERT");
            }

            stmt.executeUpdate("create or replace table user_prod(id STRING, name STRING)");
            stmt.executeUpdate("INSERT INTO user_prod(id,name) SELECT id, name FROM user_stream");

            // select a stream wont update the offset
            rs = stmt.executeQuery("SELECT * from user_stream");
            while (rs.next()) {
                System.out.println(rs.getString("NAME") + " = " + rs.getString("METADATA$ACTION"));

                String action = rs.getString("METADATA$ACTION");
                Assert.assertTrue(action.equals("INSERT") ? rs.getString("NAME").equals("test_user_updated") : rs.getString("NAME").equals("test_user"));
            }

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
            stmt.executeUpdate("drop table user");
            stmt.executeUpdate("drop stream user_stream");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBUtility.close(stmt);
        }

        // close the connection
        DBUtility.close(con);
    }
}
