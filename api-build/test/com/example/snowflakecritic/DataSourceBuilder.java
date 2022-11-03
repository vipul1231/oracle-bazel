package com.example.snowflakecritic;

import com.example.core2.ConnectionParameters;
import com.example.db.MockResultSet;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class DataSourceBuilder {

    private Statement mockStatement = mock(Statement.class);
    private PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    private SnowflakeSourceCredentials credentials = new SnowflakeSourceCredentials();
    private ConnectionParameters params;

    public DataSourceBuilder sourceCredentials(SnowflakeSourceCredentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public DataSourceBuilder params(ConnectionParameters params) {
        this.params = params;
        return this;
    }

    public Connection connection() {
        Connection mockConnection = mock(Connection.class);
        try {
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        } catch (Exception ignore) {
            // to satisfy mockito
        }

        return mockConnection;
    }

    public SnowflakeSource build() {
        SnowflakeSource snowflakeSource =
                spy(
                        new SnowflakeSource(
                                Objects.requireNonNull(credentials, "credentials"),
                                Objects.requireNonNull(params, "params")));
        try {
            doAnswer(invocation -> connection()).when(snowflakeSource).connection();

//            DataSource mockDataSource = dataSource();
//            when(snowflakeSource.getDataSource()).thenReturn(mockDataSource);
            doAnswer(invocation -> dataSource()).when(snowflakeSource).dataSource();
        } catch (Exception ignore) {
            // to satisfy mockito
        }

        return snowflakeSource;
    }

    public DataSource dataSource() {
        DataSource dataSource = mock(DataSource.class);

        try {
            Connection mockConnection = connection();
            when(dataSource.getConnection()).thenReturn(mockConnection);
            when(dataSource.getConnection(anyString(), anyString())).thenReturn(mockConnection);
        } catch (Exception ignore) {
            // to satisfy mockito
        }

        return dataSource;
    }

    public DataSourceBuilder executeQuery(String sql, String[] names, Object[][] rows) {
        try {
            when(mockStatement.executeQuery(sql)).thenReturn(resultSetFrom(names, rows));
        } catch (SQLException ignore) {
            // to satisfy mockito
        }

        return this;
    }

    public DataSourceBuilder executePreparedStatementQuery(String[] names, Object[][] rows) {
        try {
            ResultSet rs = resultSetFrom(names, rows);
            when(mockPreparedStatement.executeQuery()).thenReturn(rs);
        } catch (SQLException ignore) {
            // to satisfy mockito
        }

        return this;
    }

    public DataSourceBuilder execute(String sql, Boolean result) {
        try {
            when(mockStatement.execute(sql)).thenReturn(result);
        } catch (SQLException ignore) {
            // to satisfy mockito
        }

        return this;
    }

    public DataSourceBuilder execute(String sql, SQLException exception) {
        try {
            when(mockStatement.execute(sql)).thenThrow(exception);
        } catch (SQLException ignore) {
            // to satisfy mockito
        }

        return this;
    }

    public static String[] columns(String... names) {
        return names;
    }

    public static Object[] row(Object... values) {
        return values;
    }

    public static <T> Object[][] rows(Function<T, Object[]> f, T... objects) {
        Object[][] rows = new Object[objects.length][];

        for (int i = 0; i < objects.length; ++i) {
            rows[i] = f.apply(objects[i]);
        }

        return rows;
    }

    public static Object[][] rows(Object[]... rows) {
        return rows;
    }

    /**
     * @param ts timestamp in format <code>yyyy-[m]m-[d]d hh:mm:ss[.f...]</code>. The fractional seconds may be omitted.
     *     The leading zero for <code>mm</code> and <code>dd</code> may also be omitted.
     */
    public static Timestamp timestamp(String ts) {
        return Timestamp.valueOf(ts);
    }

    /**
     * @param date in the format "yyyy-[m]m-[d]d"
     * @return
     */
    public static java.sql.Date date(String date) {
        return java.sql.Date.valueOf(date);
    }

    /**
     * @param time in the format "hh:mm:ss"
     * @return
     */
    public static java.sql.Time time(String time) {
        return java.sql.Time.valueOf(time);
    }

    public static byte[] binary(String str) {
        return str.getBytes();
    }

    public static Long number(long l) {
        return Long.valueOf(l);
    }

    public static BigDecimal number(double d) {
        return BigDecimal.valueOf(d);
    }

    public static ResultSet resultSetFrom(String[] names, Object[] row) {
        Object[][] rows = new Object[1][row.length];
        rows[0] = row;
        return resultSetFrom(names, rows);
    }

    public static ResultSet resultSetFrom(String[] names, Object[][] rows) {
        List<Map<String, Object>> data = new ArrayList<>();

        // If non-empty result set
        if (rows.length > 0 && rows[0].length > 0) {
            for (int i = 0; i < rows.length; ++i) {
                Map<String, Object> rowMap = new HashMap<>();
                for (int j = 0; j < names.length; ++j) {
                    rowMap.put(names[j], rows[i][j]);
                }
                data.add(rowMap);
            }
        }

        return new SelfRewindingMockResultSet(data);
    }

    public static Statement statement(ResultSet rs, String sql) throws SQLException {
        Statement mock = mock(Statement.class);
        when(mock.executeQuery(sql)).thenReturn(rs);
        return mock;
    }

    public static Connection connection(Statement statement) throws SQLException {
        Connection mock = mock(Connection.class);
        when(mock.createStatement()).thenReturn(statement);
        return mock;
    }

    static class SelfRewindingMockResultSet extends MockResultSet {

        public SelfRewindingMockResultSet(List<Map<String, Object>> rows) {
            super(rows);
        }

        private String verifyColumn(String col) {
            if (!head.containsKey(col)) {
                throw new RuntimeException("Column not found: " + col);
            }
            return col;
        }

        @Override
        public String getString(String columnLabel) {
            Object val = head.get(verifyColumn(columnLabel));
            return val != null ? val.toString() : null;
        }

        @Override
        public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
            Object val = head.get(verifyColumn(columnLabel));
            return val != null ? new BigDecimal(val.toString()) : null;
        }

        @Override
        public int getInt(String columnLabel) {
            Object val = head.get(verifyColumn(columnLabel));
            return val != null ? Integer.parseInt(val.toString()) : 0;
        }

        @Override
        public long getLong(String columnLabel) {
            Object val = head.get(verifyColumn(columnLabel));
            return val != null ? Long.valueOf(val.toString()) : 0;
        }

        @Override
        public boolean getBoolean(String columnLabel) {
            Object val = head.get(verifyColumn(columnLabel));
            return val != null ? Boolean.valueOf(val.toString()) : false;
        }

        @Override
        public Timestamp getTimestamp(String columnLabel) {
            return (Timestamp) head.get(verifyColumn(columnLabel));
        }

        @Override
        public Time getTime(String columnLabel) throws SQLException {
            return (Time) head.get(verifyColumn(columnLabel));
        }

        @Override
        public Date getDate(String columnLabel) {
            return (Date) head.get(verifyColumn(columnLabel));
        }

        @Override
        public byte[] getBytes(String columnLabel) throws SQLException {
            return (byte[]) head.get(verifyColumn(columnLabel));
        }

        @Override
        public boolean next() {
            boolean next = super.next();

            if (next == false) {
                rewind();
            }

            return next;
        }
    }
}

