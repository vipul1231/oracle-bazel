package com.example.oracle;

import com.example.db.DbHostingProvider;
import com.example.db.DbServiceType;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:55 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public enum OracleServiceType implements DbServiceType {
    ORACLE(
            "oracle",
            "Oracle",
            "Oracle Database is a database system widely used to keep in-house custom data",
            DbHostingProvider.OTHER),
    ORACLE_RDS(
            "oracle_rds",
            "Oracle RDS",
            "Oracle Database RDS is a hosted version of the popular Oracle database on the Amazon RDS platform",
            DbHostingProvider.RDS),
    ORACLE_RAC(
            "oracle_rac",
            "Oracle RAC",
            "Oracle Real Application Cluster is a database system widely used to keep in-house custom data",
            DbHostingProvider.OTHER),
    ORACLE_EBS(
            "oracle_ebs",
            "Oracle EBS",
            "Oracle E-Business Suite is an enterprise resource planning system developed by Oracle",
            DbHostingProvider.OTHER);

    private final String id;
    private final String fullName;
    private final String description;
    private final DbHostingProvider hostingProvider;

    OracleServiceType(String id, String fullName, String description, DbHostingProvider hostingProvider) {
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
        throw new UnsupportedOperationException("Oracle as a warehouse doesn't exist yet");
    }
}
