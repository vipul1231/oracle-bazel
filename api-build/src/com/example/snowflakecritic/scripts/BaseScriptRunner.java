package com.example.snowflakecritic.scripts;

import com.example.snowflakecritic.SnowflakeSource;

public abstract class BaseScriptRunner implements Runnable {

    protected SnowflakeSource source;
    protected JsonFileHelper fileHelper;

    public BaseScriptRunner(SnowflakeSource source, JsonFileHelper fileHelper) {
        this.source = source;
        this.fileHelper = fileHelper;
    }
}
