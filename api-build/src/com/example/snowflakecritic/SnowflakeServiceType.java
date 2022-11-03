package com.example.snowflakecritic;

import com.example.db.DbHostingProvider;
import com.example.db.DbServiceType;

public enum SnowflakeServiceType implements DbServiceType {
    SNOWFLAKE("snowflake", "Snowflake", "Cloud Database Service (https://www.snowflake.com/)", DbHostingProvider.OTHER);

    private final String id;
    private final String fullName;
    private final String description;
    private final DbHostingProvider hostingProvider;

    SnowflakeServiceType(String id, String fullName, String description, DbHostingProvider hostingProvider) {
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
        return "";
    }

    public SnowflakeConnectorService newService() {
        return new SnowflakeConnectorService(this);
    }
}
