package com.example.snowflakecritic;

import org.junit.After;

public class TestBase {
    protected SnowflakeConnectorServiceTestContext testContext =
            new SnowflakeConnectorServiceTestContext(SnowflakeServiceType.SNOWFLAKE.newService());

    @After
    public final void cleanupAfter() {
        testContext.cleanup();
    }
}
