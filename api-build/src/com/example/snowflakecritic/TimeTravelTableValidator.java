package com.example.snowflakecritic;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class TimeTravelTableValidator implements SnowflakeTableValidator {
    private static final String EXCLUSION_REASON_CHANGE_TRACKING_NOT_ENABLED = "change tracking is not enabled";
    private final SnowflakeSourceCredentials snowflakeSourceCredentials;

    public TimeTravelTableValidator(SnowflakeSourceCredentials snowflakeSourceCredentials) {
        this.snowflakeSourceCredentials = snowflakeSourceCredentials;
    }

    @Override
    public List<String> getExclusionReasons(SnowflakeTableInfo tableInfo) {
        ImmutableList.Builder<String> reasons = ImmutableList.builder();
        if (!tableInfo.isChangeTrackingEnabled()) {
            reasons = reasons.add(EXCLUSION_REASON_CHANGE_TRACKING_NOT_ENABLED);
        }
        return reasons.build();
    }
}
