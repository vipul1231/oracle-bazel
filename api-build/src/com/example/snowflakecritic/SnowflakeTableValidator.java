package com.example.snowflakecritic;

import java.util.List;

public interface SnowflakeTableValidator {

    /**
     * If now exclusions then return an empty list.
     *
     * @param tableInfo
     * @return
     */
    List<String> getExclusionReasons(SnowflakeTableInfo tableInfo);
}
