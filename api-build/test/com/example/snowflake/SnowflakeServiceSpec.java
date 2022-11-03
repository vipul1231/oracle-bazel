package com.example.snowflake;

import com.example.core.*;
import com.example.core2.Output;
import com.example.snowflake.util.DBUtility;
import com.snowflake.client.jdbc.SnowflakeDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class SnowflakeServiceSpec {

    public static final String SCHEMA_PUBLIC = "PUBLIC";
    SnowflakeDriver snowflakeDriver = new SnowflakeDriver();
    Connection con = null;

    @Before
    public void before() {
        Statement stmt = null;
        try {
            con = snowflakeDriver.getConnection();
            stmt = con.createStatement();
            stmt.executeUpdate("create or replace table test_tbl1(id NUMBER, name STRING)");
            stmt.executeUpdate("create or replace stream test_tbl1_stream on table test_tbl1 append_only = false show_initial_rows = true");

            addData(stmt);
        } catch (SQLException ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            DBUtility.close(stmt);
        }
    }

    private void addData(Statement stmt) throws SQLException {
        stmt.executeUpdate("insert into test_tbl1 VALUES (1, 'data 1')");
        stmt.executeUpdate("insert into test_tbl1 VALUES (2, 'data 2')");
        stmt.executeUpdate("insert into test_tbl1 VALUES (3, 'data 3')");
    }


    @Test
    public void testUpdate() {
        SnowflakeService service = new SnowflakeService();
        com.example.sql_server.ConnectionParameters connectionParameters = new ConnectionParameters(null, null, null);
        connectionParameters.schema = SCHEMA_PUBLIC;

        StandardConfig standardConfig = new StandardConfig();

        TableConfig tableConfig = new TableConfig();
        SortedMap<String, TableConfig> tableConfigMap = new TreeMap<>();
        tableConfigMap.put("test_tbl1", tableConfig);

        SortedMap<String, SchemaConfig> schemas = new TreeMap<>();
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setTables(tableConfigMap);

        schemas.put(SCHEMA_PUBLIC, schemaConfig);
        standardConfig.setSchemas(schemas);


        SnowflakeState state = new SnowflakeState();
        service.update(getCredentials(), state, connectionParameters, new Output<>(), standardConfig, null);
    }

    /**
     * TODO move to a super class
     * Schema is in connectionParameters
     *
     * @return SnowflakeCredentials
     */
    protected SnowflakeCredentials getCredentials() {
        SnowflakeCredentials credentials = new SnowflakeCredentials();

        credentials.storageGcpServiceAccount = Optional.of("IPA04645");
        credentials.setConnectionString("jdbc:snowflake://ipa04645.snowflakecomputing.com");

        credentials.user = "jjnewyork123";
        credentials.password = "Newyork12345678";
        credentials.database = Optional.of("EXAMPLE_CONNECTOR_DB");

        return credentials;
    }

    @After
    public void after() {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("drop table test_tbl1");
            stmt.executeUpdate("drop stream test_tbl1_stream");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBUtility.close(stmt);

            // close the connection
            DBUtility.close(con);
        }
    }
}
