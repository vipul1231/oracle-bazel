package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core2.ConnectionParameters;
import com.example.db.Retry;
import com.example.db.SqlDbSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;

public class SqlServerSource extends SqlDbSource<DbCredentials> {

//    private static final DataSourceInitRetrier RETRIER =
//            new DataSourceInitRetrier(TimeoutException.class, exampleCertificateException.class);
    private static final DataSourceInitRetrier RETRIER = null;

    public SqlServerSource(DbCredentials originalCredentials, ConnectionParameters params) {
        super(originalCredentials, params);
    }

    public SqlServerSource(DbCredentials originalCredentials, ConnectionParameters params, DataSource dataSource) {
        super(originalCredentials, params, dataSource);
    }

    @Override
    protected DataSource nonLoggableDataSource(
            DbCredentials originalCredentials, DbCredentials credentials, com.example.core2.ConnectionParameters params) {

//        Callable<DataSource> dataSourceInit =
//                () -> {
//                    DataSource dataSource =
//                            SqlServerConnect.INSTANCE.connectDirectly(
//                                    credentials.host,
//                                    credentials.port,
//                                    credentials.user,
//                                    credentials.password,
//                                    credentials.database.orElseThrow(
//                                            () -> new RuntimeException("database is required for SQL server")),
//                                    allowPlainText(),
//                                    false,
//                                    new Properties(),
//                                    params, false);
//                                    DB.source(),
//                                    false
//                                    originalCredentials.host
//                            );

//                    return FeatureFlag.check("NoHikariConnectionPool")
//                            ? dataSource
//                            : createPoolingDataSource(dataSource);
                    return dataSource;
//                };

//        try {
//            return RETRIER.get(dataSourceInit, 3);
//            return null;
//        } catch (Exception e) {
//            Retry.FinalExceptionThrower<RuntimeException, RuntimeException> finalExceptionThrower =
//                    baseFinalExceptionThrower();

//            finalExceptionThrower.accept(e);
            // throw a RuntimeException here in case finalExceptionThrower is ineffective
//            throw new RuntimeException(e);
//        }
    }

//    private static HikariDataSource createPoolingDataSource(DataSource dataSource) {
//        HikariConfig c = new HikariConfig();
//
//        c.setMaximumPoolSize(5);
//        c.setDataSource(dataSource);
//        return new HikariDataSource(c);
//    }

    @Override
    public Connection connection() throws SQLException {
        Connection connection = super.connection();

        // Auto-commit must be disabled because otherwise the whole result set will be fetched for each table.
        // setFetchSize will not take effect without disabling auto-commit
        connection.setAutoCommit(false);

        return connection;
    }
}
