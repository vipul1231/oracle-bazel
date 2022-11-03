package com.example.snowflakecritic;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class SnowflakeIbfTableValidator implements SnowflakeTableValidator {
    @Override
    public List<String> getExclusionReasons(SnowflakeTableInfo tableInfo) {
        ImmutableList.Builder<String> reasons = ImmutableList.builder();
        return reasons.build();
    }
}