package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core2.Output;

public abstract class SnowflakeConfigurationAware {
    protected final SnowflakeConnectorServiceConfiguration serviceConfig;

    public SnowflakeConfigurationAware(SnowflakeConnectorServiceConfiguration serviceConfig) {
        this.serviceConfig = serviceConfig;
    }

    protected SnowflakeSource getSnowflakeSource() {
        return serviceConfig.getSnowflakeSource();
    }

    protected SnowflakeConnectorState getSnowflakeConnectorState() {
        return serviceConfig.getConnectorState();
    }

    protected SnowflakeInformer getSnowflakeInformer() {
        return serviceConfig.getSnowflakeInformer();
    }

    protected Output<SnowflakeConnectorState> getOutput() {
        return serviceConfig.getOutput();
    }

    protected SnowflakeSystemInfo getSnowflakeSystemInfo() {
        return serviceConfig.getSnowflakeSystemInfo();
    }

    protected StandardConfig getStandardConfig() {
        return serviceConfig.getStandardConfig();
    }
}
