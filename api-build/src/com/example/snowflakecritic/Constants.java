package com.example.snowflakecritic;

public interface Constants {
    int MAX_NUM_PRECISION = 38;
    /**
     * double max 1.7976931348623157e308 double min 4.9e-324
     *
     * <p>float max 3.4028235e38f float min 1.4e-45f
     */
    int MAX_NUM_SCALE = 37;

    int LONG_MAX_PRECISION = 18;
    int DOUBLE_MAX_PRECISION = 15;
    int INTEGER_MAX_PRECISION = 9;
    int FLOAT_MAX_PRECISION = 7;
}
