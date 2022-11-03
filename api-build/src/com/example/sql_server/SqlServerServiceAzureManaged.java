package com.example.sql_server;

import static com.example.sql_server.SqlServerServiceType.SQL_SERVER_AZURE_MANAGED;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SqlServerServiceAzureManaged extends SqlServerService {

    public SqlServerServiceAzureManaged() {
        serviceType = SQL_SERVER_AZURE_MANAGED;
    }

    @Override
    public Path largeIcon() {
        return Paths.get("/integrations/sql_server/resources/azure.png");
    }

    @Override
    public URI linkToDocs() {
        return URI.create("/docs/databases/sql-server/azure-managed-setup-guide");
    }
}
