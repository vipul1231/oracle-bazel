package com.example.oracle;

import com.example.db.SqlDbSource;
import com.example.db.TunnelableConnectionException;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 9:49 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class LoggableDataSource {
    public LoggableDataSource(DataSource nonLoggableDataSource, boolean b) throws TunnelableConnectionException {
    }

    public Connection getConnection() {
        return null;
    }
}