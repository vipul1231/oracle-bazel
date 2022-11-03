package com.example.snowflakecritic;

public interface SnowflakeTableUpdater {

    void incrementalUpdate();

    SnowflakeSourceCredentials.UpdateMethod getUpdateMethod();

    boolean canUpdate();

    /**
     * Perform any work related to the table if it has not yet been imported. This object knows its own state so it will
     * only perform work if the table is in the pre import state.
     */
    void performPreImport();
}
