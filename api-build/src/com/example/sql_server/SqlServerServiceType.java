package com.example.sql_server;

import com.example.db.DbHostingProvider;
import com.example.db.DbServiceType;

public enum SqlServerServiceType implements DbServiceType {
    SQL_SERVER(
            "sql_server",
            "SQL Server",
            "Microsoft SQL Server is a database system widely used to keep in-house custom data",
            DbHostingProvider.OTHER),
    SQL_SERVER_RDS(
            "sql_server_rds",
            "SQL Server RDS",
            "Microsoft SQL Server RDS is a hosted version of the popular Microsoft SQL Server database on the Amazon RDS platform",
            DbHostingProvider.RDS),
    SQL_SERVER_AZURE(
            "azure_sql_db",
            "Azure SQL Database",
            "Azure SQL Database is a managed relational cloud database service",
            DbHostingProvider.AZURE),
    SQL_SERVER_AZURE_MANAGED(
            "azure_sql_managed_db",
            "Azure SQL Managed Instance",
            "Azure SQL Managed Instance is a managed relational cloud database service",
            DbHostingProvider.AZURE),
    DYNAMICS_365_FO(
            "dynamics_365_fo",
            "Dynamics 365 Finance and Operations",
            "An Enterprise Resource Planning system for medium to large businesses",
            DbHostingProvider.AZURE);

    private final String id;
    private final String fullName;
    private final String description;
    private final DbHostingProvider hostingProvider;

    SqlServerServiceType(String id, String fullName, String description, DbHostingProvider hostingProvider) {
        this.id = id;
        this.fullName = fullName;
        this.description = description;
        this.hostingProvider = hostingProvider;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String fullName() {
        return fullName;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public DbHostingProvider hostingProvider() {
        return hostingProvider;
    }

    @Override
    public String warehouseId() {
        switch (this) {
            case SQL_SERVER_AZURE:
                return "azure_sql_database"; // TODO rename this to follow `_warehouse` naming convention
            default:
                return this.id + "_warehouse";
        }
    }
}
