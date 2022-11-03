package com.example.snowflakecritic.scripts;

import com.example.snowflakecritic.SnowflakeSource;
import com.example.snowflakecritic.SnowflakeSystemInfo;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class GetSystemTime extends BaseScriptRunner {

    public GetSystemTime(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
    }

    @Override
    public void run() {
        try {
            SnowflakeSystemInfo systemInfo = new SnowflakeSystemInfo(source);
            source.executeWithRetry(
                    connection -> {
                        systemInfo.logSessionInfo(connection);
                        String sysTime = systemInfo.getSystemTime().toString();
                        System.out.println("\nSnowflake system time " + sysTime);
                    });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
