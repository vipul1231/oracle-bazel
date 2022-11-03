package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core2.ConnectionParameters;
import com.example.core2.Output;
import com.example.snowflakecritic.scripts.ImportTable;
import com.example.snowflakecritic.scripts.JsonFileHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.example.snowflakecritic.SnowflakeConnectorTestUtil.tableInfoBuilder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.inOrder;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.hamcrest.Matchers.hasEntry;

public class SnowflakeScriptImportTableSpec {
    private SnowflakeConnectorServiceTestContext testContext =
            new SnowflakeConnectorServiceTestContext(SnowflakeServiceType.SNOWFLAKE.newService());
    private SnowflakeTableInfo testTableInfo = tableInfoBuilder("PUBLIC", "table1")
            .pkCol("ID", SnowflakeType.NUMBER)
            .col("COL2", SnowflakeType.TEXT)
            .build();

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-4s] %5$s %n");
    }

//    @Before
//    public void setup() throws IOException {
//        testContext
//                .setStandardConfig(mock(StandardConfig.class))
//                .setConnectionParameters(new ConnectionParameters(null, "PUBLIC", null))
//                //.setSnowflakeInformer(mockInformer(testTableInfo))
//                .setCredentials(new JsonFileHelper(new File(
//                        System.getProperty("workspace.dir") + File.separator + "testing_playground")).loadCredentials());
//
//        testContext.getSource().make(testTableInfo);
//    }
//
//    @After
//    public void cleanup() {
//        testContext.getSource().drop(testTableInfo.sourceTable.getName());
//    }

//    @Test
//    public void testImportPage() throws IllegalAccessException {
//        insert(testContext.getSource(), testTableInfo, 1, "data1");
//        insert(testContext.getSource(), testTableInfo, 2, "data2");
//        insert(testContext.getSource(), testTableInfo, 3, "data3");
//
//        Output<SnowflakeConnectorState> mockOutput = mock(Output.class);
//        ImportTable importTable  = new ImportTable(
//                testContext.getSource(),
//                new JsonFileHelper(new File(System.getProperty("workspace.dir") + File.separator + "testing_playground")));
//        importTable.setOutput(mockOutput);
//
//        importTable.run();
//
//        InOrder inOrder = inOrder(mockOutput);
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)1L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)2L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)3L)));
//        verifyNoMoreInteractions(mockOutput);
//        testContext.getSource().delete(testTableInfo);
//    }
//
//    @Test
//    public void testImportPageWithInsert() {
//        insert(testContext.getSource(), testTableInfo, 1, "data1");
//        insert(testContext.getSource(), testTableInfo, 2, "data2");
//        insert(testContext.getSource(), testTableInfo, 3, "data3");
//        insert(testContext.getSource(), testTableInfo, 4, "data3");
//        insert(testContext.getSource(), testTableInfo, 5, "data3");
//        insert(testContext.getSource(), testTableInfo, 6, "data3");
//
//        ExecutorService executor = Executors.newFixedThreadPool(10);
//        executor.submit(() -> {
//            for (int i = 0; i < 10; i++) {
//                insert(testContext.getSource(), testTableInfo, i + 100, "data3");
//                SnowflakeConnectorTestUtil.sleep(10);
//            }
//        });
//
//        Output<SnowflakeConnectorState> mockOutput = mock(Output.class);
//        ImportTable importTable  = new ImportTable(
//                testContext.getSource(),
//                new JsonFileHelper(new File(System.getProperty("workspace.dir") + File.separator + "testing_playground")));
//        importTable.setOutput(mockOutput);
//
//        importTable.run();
//
//        InOrder inOrder = inOrder(mockOutput);
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)1L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)2L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)3L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)4L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)5L)));
//        inOrder.verify(mockOutput).upsert(any(), (Map<String, Object>)argThat(hasEntry("ID", (Object)6L)));
//        verifyNoMoreInteractions(mockOutput);
//
//        testContext.getSource().delete(testTableInfo);
//    }
}
