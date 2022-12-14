package com.example.sql_server;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LoggableCallableStatement extends Queryable implements CallableStatement {
    private final CallableStatement wrappedCallableStatement;
    private final Map<String, String> namedValues = new HashMap<>();
    final String initialSql;

    protected Optional<String> wrappedStatement() {
        return Optional.of(wrappedCallableStatement.toString());
    }

    LoggableCallableStatement(CallableStatement wrappedCallableStatement, String initialSql) {
        this(wrappedCallableStatement, initialSql, LoggableConfig.defaultConfig());
    }

    public LoggableCallableStatement(
            CallableStatement wrappedCallableStatement, String initialSql, LoggableConfig config) {
        super(config);
        this.wrappedCallableStatement = wrappedCallableStatement;
        this.initialSql = initialSql;
    }

    private void saveQueryNamedParam(String name, Object obj) {
        String strValue = convertObjectToString(obj);

        namedValues.put(name, strValue);
    }

    // TODO write tests for this
    private String addNamedParamsToSql(String sql) {
        // Replace `:<name>` with values for CallableStatement
        for (String name : namedValues.keySet()) {
            String formattedName = ":" + name;
            if (!sql.contains(formattedName))
                throw new RuntimeException("Named parameter could not be found in sql string");

            sql = sql.replaceFirst(formattedName, namedValues.get(name));
        }
        return sql;
    }

    @Override
    void addQueryToBatch(String rawSql) {
        batchedSqlQueries.add(processQuery(rawSql));
        parameterValues.clear();
        namedValues.clear();
    }

    @Override
    String processQuery(String rawQuery) {
        String partiallyProcessed = addIndexedParamsToSql(rawQuery);
        return addNamedParamsToSql(partiallyProcessed);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterIndex, sqlType, scale);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wrappedCallableStatement.wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getString(parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getBoolean(parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getByte(parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getShort(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getInt(parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getLong(parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getFloat(parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getDouble(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return wrappedCallableStatement.getBigDecimal(parameterIndex, scale);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getBytes(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getDate(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getTime(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getTimestamp(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getObject(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getBigDecimal(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return wrappedCallableStatement.getObject(parameterIndex, map);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getRef(parameterIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getBlob(parameterIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getClob(parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getArray(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getDate(parameterIndex, cal);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getTime(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getTimestamp(parameterIndex, cal);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        wrappedCallableStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getURL(parameterIndex);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        saveQueryNamedParam(parameterName, val);
        wrappedCallableStatement.setURL(parameterName, val);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        saveQueryNamedParam(parameterName, sqlType);
        wrappedCallableStatement.setNull(parameterName, sqlType);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBoolean(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setByte(parameterName, x);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setShort(parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setInt(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setLong(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setFloat(parameterName, x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setDouble(parameterName, x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBigDecimal(parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setString(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBytes(parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setDate(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setTime(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setTimestamp(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setObject(parameterName, x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setObject(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setDate(parameterName, x, cal);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setTime(parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setTimestamp(parameterName, x, cal);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        saveQueryNamedParam(parameterName, sqlType);
        wrappedCallableStatement.setNull(parameterName, sqlType, typeName);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return wrappedCallableStatement.getString(parameterName);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return wrappedCallableStatement.getBoolean(parameterName);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return wrappedCallableStatement.getByte(parameterName);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return wrappedCallableStatement.getShort(parameterName);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return wrappedCallableStatement.getInt(parameterName);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return wrappedCallableStatement.getLong(parameterName);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return wrappedCallableStatement.getFloat(parameterName);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return wrappedCallableStatement.getDouble(parameterName);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return wrappedCallableStatement.getBytes(parameterName);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return wrappedCallableStatement.getDate(parameterName);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return wrappedCallableStatement.getTime(parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return wrappedCallableStatement.getTimestamp(parameterName);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return wrappedCallableStatement.getObject(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return wrappedCallableStatement.getBigDecimal(parameterName);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return wrappedCallableStatement.getObject(parameterName, map);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return wrappedCallableStatement.getRef(parameterName);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return wrappedCallableStatement.getBlob(parameterName);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return wrappedCallableStatement.getClob(parameterName);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return wrappedCallableStatement.getArray(parameterName);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getDate(parameterName, cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getTime(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return wrappedCallableStatement.getTimestamp(parameterName, cal);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return wrappedCallableStatement.getURL(parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return wrappedCallableStatement.getRowId(parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setRowId(parameterName, x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        saveQueryNamedParam(parameterName, value);
        wrappedCallableStatement.setNString(parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        saveQueryNamedParam(parameterName, value);
        wrappedCallableStatement.setNCharacterStream(parameterName, value, length);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        saveQueryNamedParam(parameterName, value);
        wrappedCallableStatement.setNClob(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setClob(parameterName, reader, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        saveQueryNamedParam(parameterName, inputStream);
        wrappedCallableStatement.setBlob(parameterName, inputStream, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setNClob(parameterName, reader, length);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return wrappedCallableStatement.getNClob(parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        saveQueryNamedParam(parameterName, xmlObject);
        wrappedCallableStatement.setSQLXML(parameterName, xmlObject);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return wrappedCallableStatement.getSQLXML(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return wrappedCallableStatement.getNString(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return wrappedCallableStatement.getNCharacterStream(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return wrappedCallableStatement.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return wrappedCallableStatement.getCharacterStream(parameterName);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBlob(parameterName, x);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setClob(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setAsciiStream(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        saveQueryNamedParam(parameterName, x);
        wrappedCallableStatement.setBinaryStream(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setCharacterStream(parameterName, reader);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        saveQueryNamedParam(parameterName, value);
        wrappedCallableStatement.setNCharacterStream(parameterName, value);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setClob(parameterName, reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        saveQueryNamedParam(parameterName, inputStream);
        wrappedCallableStatement.setBlob(parameterName, inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        saveQueryNamedParam(parameterName, reader);
        wrappedCallableStatement.setNClob(parameterName, reader);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return wrappedCallableStatement.getObject(parameterIndex, type);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return wrappedCallableStatement.getObject(parameterName, type);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        logQuery(initialSql);
        ResultSet rs = timeQuery(wrappedCallableStatement::executeQuery);
        return new LoggableResultSet(rs, config, queryNumber);
    }

    @Override
    public int executeUpdate() throws SQLException {
        logQuery(initialSql);
        return timeQuery(wrappedCallableStatement::executeUpdate);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        saveQueryIndexedParam(parameterIndex, null);
        wrappedCallableStatement.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        wrappedCallableStatement.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        logQuery(initialSql);
        return timeQuery(wrappedCallableStatement::execute);
    }

    @Override
    public void addBatch() throws SQLException {
        addQueryToBatch(initialSql);
        wrappedCallableStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return wrappedCallableStatement.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        saveQueryIndexedParam(parameterIndex, null);
        wrappedCallableStatement.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return wrappedCallableStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        saveQueryIndexedParam(parameterIndex, value);
        wrappedCallableStatement.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, value);
        wrappedCallableStatement.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        saveQueryIndexedParam(parameterIndex, value);
        wrappedCallableStatement.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, inputStream);
        wrappedCallableStatement.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        saveQueryIndexedParam(parameterIndex, xmlObject);
        wrappedCallableStatement.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        saveQueryIndexedParam(parameterIndex, x);
        wrappedCallableStatement.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        saveQueryIndexedParam(parameterIndex, value);
        wrappedCallableStatement.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        saveQueryIndexedParam(parameterIndex, inputStream);
        wrappedCallableStatement.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        saveQueryIndexedParam(parameterIndex, reader);
        wrappedCallableStatement.setNClob(parameterIndex, reader);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logQuery(sql);
        ResultSet rs = timeQuery(() -> wrappedCallableStatement.executeQuery(sql));
        return new LoggableResultSet(rs, config, queryNumber);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.executeUpdate(sql));
    }

    @Override
    public void close() throws SQLException {
        if (config.closeResourcesQuietly) {
            Queryable.closeQuietly(wrappedCallableStatement);
        } else {
            wrappedCallableStatement.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return wrappedCallableStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        wrappedCallableStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return wrappedCallableStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        wrappedCallableStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        wrappedCallableStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return wrappedCallableStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        wrappedCallableStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        wrappedCallableStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrappedCallableStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrappedCallableStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        wrappedCallableStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.execute(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return new LoggableResultSet(wrappedCallableStatement.getResultSet(), config, queryNumber);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return wrappedCallableStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return wrappedCallableStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        wrappedCallableStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return wrappedCallableStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        wrappedCallableStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return wrappedCallableStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return wrappedCallableStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return wrappedCallableStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        addQueryToBatch(sql);
        wrappedCallableStatement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        clearBatchedQueries();
        wrappedCallableStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        logBatchedQueries();
        return wrappedCallableStatement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return wrappedCallableStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return wrappedCallableStatement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new LoggableResultSet(wrappedCallableStatement.getGeneratedKeys(), config, queryNumber);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.executeUpdate(sql, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.executeUpdate(sql, columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.executeUpdate(sql, columnNames));
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.execute(sql, autoGeneratedKeys));
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.execute(sql, columnIndexes));
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        logQuery(sql);
        return timeQuery(() -> wrappedCallableStatement.execute(sql, columnNames));
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return wrappedCallableStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return wrappedCallableStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        wrappedCallableStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return wrappedCallableStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        wrappedCallableStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return wrappedCallableStatement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrappedCallableStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrappedCallableStatement.isWrapperFor(iface);
    }
}
