package com.example.snowflakecritic;

import com.example.core2.ConnectionParameters;
import com.example.core2.Output;
import com.example.oracle.WizardContext;
import com.example.snowflakecritic.setup.SnowflakeConnectorSetupForm;
import org.junit.Before;
import org.junit.Test;

import static com.example.snowflakecritic.SnowflakeConnectorTestUtil.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SnowflakeConnectorServiceSpec extends TestBase {

    @Before
    public void setup() {
        testContext.withMockSnowflakeSystemInfo();
    }

    @Test
    public void service_basicMethods() {
        SnowflakeConnectorService service = testContext.getService();

        assertEquals(SnowflakeServiceType.SNOWFLAKE.id(), service.id());
        assertEquals(SnowflakeServiceType.SNOWFLAKE, service.serviceType());
        assertNotNull(service.serviceType().fullName());
        assertNotNull(service.serviceType().description());
        assertNotNull(service.serviceType().hostingProvider());
        assertNotNull(service.serviceType().warehouseId());
        assertFalse(service.supportsCreationViaApi());
        assertEquals(SnowflakeSourceCredentials.class, service.credentialsClass());

        testContext
                .withMockStandardConfig()
                .setCredentials(newCredentials("dummy", 1, "db"))
                .setConnectionParameters(mock(ConnectionParameters.class))
                .setConnectorState(new SnowflakeConnectorState())
                .setOutput(mock(Output.class));

        SnowflakeImporter importer = testContext.getImporter();
        SnowflakeIncrementalUpdater incrementalUpdater = testContext.getIncrementalUpdater();
        assertSame(importer.getSnowflakeSource(), incrementalUpdater.getSnowflakeSource());
        assertSame(importer.getSnowflakeInformer(), incrementalUpdater.getSnowflakeInformer());
        assertSame(importer.getSnowflakeConnectorState(), incrementalUpdater.getSnowflakeConnectorState());
        assertSame(importer.getOutput(), incrementalUpdater.getOutput());

        assertNotNull(testContext.getTableExcludedReasons(mock(SnowflakeTableInfo.class)));
    }

    @Test
    public void setupForm_normalConstruction() {
        SnowflakeConnectorService service = SnowflakeServiceType.SNOWFLAKE.newService();

        SnowflakeConnectorSetupForm setupForm = service.setupForm(mock(WizardContext.class));

        assertNotNull(setupForm.form(new SnowflakeSourceCredentials()));
    }

    @Test
    public void informer_basicConstruction() {
        assertNotNull(
                testContext
                        .withMockStandardConfig()
                        .setCredentials(newCredentials("dummy", 1, "db"))
                        .setConnectionParameters(mock(ConnectionParameters.class))
                        .getInformer());
    }

    @Test
    public void service_update() {
        testContext
                .withMockStandardConfig()
                .setConnectionParameters(mock(ConnectionParameters.class))
                .setOutput(mock(Output.class))
                .setSnowflakeSource(new SnowflakeSource(newCredentials("dummy", 1, "db"), null))
                .withTestSnowflakeInformer(getTestTable1())
                .runUpdate();
    }

    @Test
    public void service_standardConfig() {
        assertNotNull(
                testContext
                        .setCredentials(newCredentials("dummy", 1, "db"))
                        .setConnectionParameters(mock(ConnectionParameters.class))
                        .withTestSnowflakeInformer(getTestTable1())
                        .getStandardConfig());
    }

    private SnowflakeTableInfo getTestTable1() {
        return tableInfoBuilder("schema", "table")
                .pkCol("ID", SnowflakeType.NUMBER)
                .col("COL2", SnowflakeType.TEXT)
                .build();
    }
}
