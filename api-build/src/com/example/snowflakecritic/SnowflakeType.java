package com.example.snowflakecritic;

import com.example.core.annotations.DataType;
import com.example.oracle.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.time.*;
import java.time.format.DateTimeFormatter;

import static com.example.snowflakecritic.Constants.*;

public enum SnowflakeType {
    NUMBER,
    FLOAT,
    TEXT,
    BOOLEAN,
    TIME,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ,
    DATE,
    VARIANT,
    BINARY,
    ARRAY,
    OBJECT,
    GEOGRAPHY;

    public static SnowflakeType fromDataTypeString(String dataType) {
        switch (dataType) {
            case "NUMBER":
                return NUMBER;
            case "FLOAT":
                return FLOAT;
            case "TEXT":
                return TEXT;
            case "BOOLEAN":
                return BOOLEAN;
            case "TIMESTAMP_NTZ":
                return TIMESTAMP_NTZ;
            case "TIMESTAMP_TZ":
                return TIMESTAMP_TZ;
            case "TIMESTAMP_LTZ":
                return TIMESTAMP_LTZ;
            case "DATE":
                return DATE;
            case "VARIANT":
                return VARIANT;
            case "BINARY":
                return BINARY;
            case "OBJECT":
                return OBJECT;
            case "GEOGRAPHY":
                return GEOGRAPHY;
            default:
                throw new IllegalArgumentException("unsupported type: " + dataType);
        }
    }

    public String random(SnowflakeColumnInfo columnInfo) {
        switch (this) {
            case NUMBER:
                return String.valueOf(RandomUtils.nextLong(0,
                        (long) Math.pow(10,
                                columnInfo.numericPrecision.orElse(3) - columnInfo.numericScale.orElse(0))));
            case FLOAT:
                return String.valueOf(RandomUtils.nextFloat(0,
                        (float) Math.pow(10,
                                columnInfo.numericPrecision.orElse(3) - columnInfo.numericScale.orElse(0))));
            case TEXT:
                return "'" + RandomStringUtils.randomAlphanumeric(columnInfo.maxLength.orElse(10)) + "'";
            case DATE:
                return String.format("to_date('%s')", DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        .withZone(ZoneId.systemDefault()).format(Instant.ofEpochSecond(
                                RandomUtils.nextLong(
                                        Instant.now().getEpochSecond(),
                                        Instant.now().plus(Duration.ofDays(100)).getEpochSecond()))));
            case BOOLEAN:
            case TIMESTAMP_NTZ:
            case TIMESTAMP_TZ:
            case TIMESTAMP_LTZ:
            case VARIANT:
            case BINARY:
                return "null";
            default:
                throw new IllegalArgumentException("unrecognized type: " + this);
        }
    }

    public DataType destinationType() {
        switch (this) {
            case NUMBER:
                return DataType.BigDecimal;
            case FLOAT:
                return DataType.Double;
            case TEXT:
                return DataType.String;
            case BOOLEAN:
                return DataType.Boolean;
            case TIMESTAMP_NTZ:
                return DataType.LocalDateTime;
            case TIMESTAMP_TZ:
            case TIMESTAMP_LTZ:
                return DataType.Instant;
            case TIME:
                return DataType.Time;
            case DATE:
                return DataType.LocalDate;
            case VARIANT:
                return DataType.Json;
            case BINARY:
                return DataType.Binary;
            default:
                throw new IllegalArgumentException("unrecognized type: " + this);
        }
    }

    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_SCALE = 37;

    public static Pair<Object, Boolean> promoteNumberColumnAndValue(SnowflakeColumnInfo column, BigDecimal value) {
        if (value == null) return new Pair<>(value, false);

        DataType outputType;
        if (value.precision() > DEFAULT_PRECISION || value.scale() > DEFAULT_SCALE) {
            outputType = DataType.String;
        } else if (value.precision() > 18) {
            outputType = DataType.BigDecimal;
        } else if (value.precision() > 9) {
            outputType = DataType.Long;
        } else {
            outputType = DataType.Int;
        }

        boolean valuePromoted = column.destinationType != outputType;

        Object promotedValue = value;

        if (valuePromoted) {
            column.destinationType = outputType;

            switch (outputType) {
                case Int:
                    promotedValue = value.intValueExact();
                    break;
                case Long:
                    promotedValue = value.longValueExact();
                    break;
                case BigDecimal:
                    promotedValue = value;
                    break;
                default:
                    promotedValue = value.stripTrailingZeros().toPlainString();
                    break;
            }
        }

        return new Pair(promotedValue, valuePromoted);
    }

    public static LocalDate toLocalDate(Date sqlDate) {
        return sqlDate.toLocalDate();
    }

    public static LocalTime toLocalTime(Time sqlTime) {
        return sqlTime.toLocalTime();
    }

    public static DataType inferNumberType(int precision, int scale) {
        if (scale > 0) {
            return inferFloatType(precision, scale);
        }

        // A zero or negative scale indicates an integer (whole number) type
        return inferIntegerType(precision);
    }

    private static DataType inferIntegerType(int precision) {
        if (precision > MAX_NUM_PRECISION) {
            return DataType.String;
        }

        // This is an integer type number
        if (precision > LONG_MAX_PRECISION) {
            return DataType.BigDecimal;
        }

        if (precision > INTEGER_MAX_PRECISION) {
            return DataType.Long;
        }

        return DataType.Int;
    }

    private static DataType inferFloatType(int precision, int scale) {
        if (scale > MAX_NUM_SCALE) {
            return DataType.String;
        }

        if (precision > DOUBLE_MAX_PRECISION) {
            return DataType.BigDecimal;
        }

        if (precision > FLOAT_MAX_PRECISION) {
            return DataType.Double;
        }

        return DataType.Float;
    }
}
