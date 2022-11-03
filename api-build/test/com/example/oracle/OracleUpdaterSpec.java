package com.example.oracle;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core.mocks.MockOutput2;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.example.oracle.Constants.MAX_INVALID_NUMBER_WARNING_BEFORE_GIVING_UP;
import static com.example.oracle.Constants.ROW_ID_COLUMN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/13/2021<br/>
 * Time: 8:46 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleUpdaterSpec {

    private OracleApi api;
    private DataSource dataSource;
    private OracleUpdater oracleUpdater;
    private OracleConnectorContext context;


    @Before
    public void beforeOracleUpdaterSpec() {
        dataSource = mock(DataSource.class);
        api = spy(new OracleApi(dataSource));
        context = mock(OracleConnectorContext.class);

        oracleUpdater = spy(new OracleUpdater(context));

        Transactions.sleepDurationFunction = Transactions.quickSleepDurationFunction; // speed up tests with retry logic
    }

    @Test
    public void acceptRows_shouldReplaceNuericOverflowWithNULL() throws SQLException {
        TableRef table = new TableRef("dummy", "dummy");
        List<OracleColumn> columns = new ArrayList<>();
        columns.add(new OracleColumn("Good", OracleType.create("NUMBER"), false, table, Optional.empty()));
        columns.add(new OracleColumn("NumericOverflow", OracleType.create("NUMBER"), false, table, Optional.empty()));
        ResultSet rows = mock(ResultSet.class);
        String strVal = "100";
        doReturn("AAAxbZZYYYDxyz").when(rows).getString(ROW_ID_COLUMN_NAME);
        doReturn(new BigDecimal(strVal)).when(rows).getBigDecimal("Good");
        doReturn("~").when(rows).getString("NumericOverflow");
        doThrow(new SQLException("Numeric Overflow")).when(rows).getBigDecimal("NumericOverflow");

        api.setOutputHelper(
                new OracleOutputHelper(
                        new MockOutput2<>(new OracleState()), "", new StandardConfig(), new OracleMetricsHelper()));

        oracleUpdater.acceptRow(
                table,
                rows,
                columns,
                (id, row) -> {
                    assertEquals(strVal, row.get("Good").toString());
                    assertEquals(null, row.get("NumericOverflow"));
                });

        try {
            for (int i = 0; i < MAX_INVALID_NUMBER_WARNING_BEFORE_GIVING_UP; i++) {
                oracleUpdater.acceptRow(
                        table,
                        rows,
                        columns,
                        (id, row) -> {
                            assertEquals(strVal, row.get("Good").toString());
                            assertEquals(null, row.get("NumericOverflow"));
                        });
            }
            fail("We should give up when there are too many invalid number warning.");
        } catch (Exception e) {
            // must fail.
        }
    }


}