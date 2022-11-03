package com.example.sql_server;

import static com.example.sql_server.SqlServerServiceType.SQL_SERVER_AZURE;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SqlServerServiceAzure extends SqlServerService {

    public SqlServerServiceAzure() {
        serviceType = SQL_SERVER_AZURE;
    }

    @Override
    public Path largeIcon() {
        return Paths.get("/integrations/sql_server/resources/azure.png");
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/databases/sql-server/azure-setup-guide");
    }
}
