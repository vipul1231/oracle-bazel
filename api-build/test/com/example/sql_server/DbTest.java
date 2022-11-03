package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.TableRef;
import com.example.db.Retry;
import com.example.db.TunnelableConnection;
import com.example.fire.migrated.sql_server.dockerized.MigratedDockerizedSqlServerInstance;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.util.TimeZone;

/** Encapsulates basic setup logic common to major database Spec classes. */
public abstract class DbTest<Creds extends DbCredentials, State> /*extends TimedTests*/ {

    protected TestSourceDb<Creds> testDb;
    protected DbRowHelper rowHelper;

    protected SqlDbTableHelper tableHelper;
    protected TableRef defaultTable;
    protected Instant testStart;

    @BeforeClass
    public static void beforeDbTestClass() {
        TunnelableConnection.usePortForwarder = false;
        // Set timezone to standardize JDBC's interpretation of timestamps
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        // reduce time spent waiting before a retry attempt
        Retry.sleepWaitDuration = Duration::ofSeconds;
    }

    @Before
    public void beforeDbTest() {
        testDb = testDb();
        rowHelper = rowHelper();

        tableHelper = tableHelper();
        defaultTable = tableHelper().defaultTable();
        testStart = Instant.now();
    }

    @After
    public final void afterDbTest() throws Exception {
        testDb.close();
    }

    @AfterClass
    public static void release() {
        MigratedDockerizedSqlServerInstance.clearInstances();
    }

    protected abstract TestSourceDb<Creds> testDb();

    protected abstract DbRowHelper rowHelper();

    protected abstract SqlDbTableHelper tableHelper();

    protected abstract State newState();

    protected abstract Class<State> stateClass();

    protected com.example.core2.ConnectionParameters params() {
        return new com.example.core2.ConnectionParameters("default_owner", "wh_prefix", TimeZone.getDefault());
    }
}
