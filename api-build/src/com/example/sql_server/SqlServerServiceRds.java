package com.example.sql_server;

import static com.example.sql_server.SqlServerServiceType.SQL_SERVER_RDS;

import java.net.URI;

public class SqlServerServiceRds extends SqlServerService {
    public SqlServerServiceRds() {
        serviceType = SQL_SERVER_RDS;
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/databases/sql-server/rds-setup-guide");
    }
}
