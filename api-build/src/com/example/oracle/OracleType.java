package com.example.oracle;

import com.example.core.annotations.DataType;
import com.example.flag.FlagName;
import com.example.oracle.exceptions.TypePromotionNeededException;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:48 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
/*
 * Docs:
 *
 * 12c (12.2.X) https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/Data-Types.html#GUID-A3C0D836-BADB-44E5-A5D4-265BA5968483
 * 12c (12.1.0.2) https://docs.oracle.com/database/121/SQLRF/sql_elements001.htm#SQLRF0021
 * 11g (11.2) https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements001.htm#SQLRF0021
 */

public class OracleType {
    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_SCALE = 37;

    public final Type type;
    public final String typeName;
    public final Integer columnId;
    public final boolean nullable;

    private Integer byteLength;
    private Integer precision;
    private Integer scale;

    OracleType(
            String typeName,
            Integer byteLength,
            Integer precision,
            Integer scale,
            Integer columnId,
            boolean nullable,
            boolean isHva) {
        if (typeName.startsWith("TIMESTAMP")) {
            typeName = typeName.replaceAll("\\([0-9]+\\)", "");
        }
        this.typeName = typeName;
        this.type = Type.fromOracleColumnType(typeName, isHva);
        this.columnId = columnId;
        this.byteLength = byteLength;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        normalize();
    }

    public OracleType(String typeName, Integer byteLength, Integer precision, Integer scale) {
        this(typeName, byteLength, precision, scale, null, false, false);
    }

    boolean isLong() {
        return type == Type.LONG;
    }

    boolean isDate() {
        return type == Type.DATE;
    }

    public boolean isLob() {
        return type == Type.NCLOB || type == Type.BLOB || type == Type.CLOB;
    }

    public boolean isNumber() {
        return type == Type.NUMBER || type == Type.FLOAT;
    }

    public boolean isCharacterString() {
        return type == Type.CHAR || type == Type.VARCHAR || type == Type.VARCHAR2;
    }

    public boolean isUnicodeString() {
        return type == Type.NCHAR || type == Type.NVARCHAR2;
    }

    public boolean isStringLike() {
        return isCharacterString() || isUnicodeString();
    }

    public boolean isDateTimeLike() {
        switch (type) {
            case DATE:
            case TIME:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;

            default:
                return false;
        }
    }

    OptionalInt getColumnByteLength() {
        if (byteLength == null || byteLength < 0) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(byteLength);
    }

    OptionalInt getColumnPrecision() {
        return (precision == null) ? OptionalInt.empty() : OptionalInt.of(precision);
    }

    OptionalInt getColumnScale() {
        return (scale == null) ? OptionalInt.empty() : OptionalInt.of(scale);
    }

    public Integer getRawByteLength() {
        return byteLength;
    }

    Integer getRawPrecision() {
        return precision;
    }

    Integer getRawScale() {
        return scale;
    }

    Instant instantFromBytes(byte[] rawBytes, OracleColumn oracleColumn) {
        switch (type) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return OracleTimestamp.instantFromBytes(rawBytes, oracleColumn);
            default:
                throw new RuntimeException("Cannot convert type `" + type.name() + "` into Instant");
        }
    }

    LocalDateTime localDateTimeFromBytes(byte[] rawBytes, OracleColumn oracleColumn) {
        switch (type) {
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return OracleTimestamp.localDateTimeFromBytes(rawBytes, oracleColumn);
            default:
                throw new RuntimeException("Cannot convert type `" + type.name() + "` into LocalDateTime");
        }
    }

    LocalDate localDateFromBytes(byte[] rawBytes, OracleColumn oracleColumn) throws TypePromotionNeededException {
        switch (type) {
            case DATE:
                return OracleTimestamp.localDateFromBytes(rawBytes, oracleColumn);
            default:
                throw new RuntimeException("Cannot convert type `" + type.name() + "` into LocalDateTime");
        }
    }

    static LocalDate localDateFromString(String rawValue, OracleColumn oracleColumn)
            throws TypePromotionNeededException {
        try {
            LocalDateTime ldt = LocalDateTime.from(localDateTimeFormatter().parse(rawValue));
            if (ldt.getHour() != 0 || ldt.getMinute() != 0 || ldt.getSecond() != 0) {
                throw new TypePromotionNeededException("Date contains time information");
            }
            return ldt.toLocalDate();
        } catch (DateTimeException e) {
            oracleColumn.badDateVals.add(rawValue);
            return null;
        }
    }

    static Instant instantFromString(String rawValue, OracleColumn oracleColumn) {
        try {
            DateTimeFormatter instantFormatter = instantFormatter();
            String hasTimeZoneOffset = ".* ?[+-][0-9]{2}:[0-9]{2}";
            if (!rawValue.matches(hasTimeZoneOffset)) {
                instantFormatter = instantFormatter.withZone(ZoneId.of("UTC"));
            }
            return Instant.from(instantFormatter.parse(rawValue));
        } catch (DateTimeException e) {
            oracleColumn.badDateVals.add(rawValue);
            return null;
        }
    }

    static LocalDateTime localDateTimeFromString(String rawValue, OracleColumn oracleColumn) {
        try {
            return LocalDateTime.from(localDateTimeFormatter().parse(rawValue));
        } catch (DateTimeException e) {
            oracleColumn.badDateVals.add(rawValue);
            return null;
        }
    }

    private static DateTimeFormatterBuilder formatterBuilderWithoutTimezone() {
        return new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .optionalStart()
                .appendLiteral("T")
                .optionalEnd()
                .optionalStart()
                .appendLiteral(" ")
                .optionalEnd()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true);
    }

    static DateTimeFormatter instantFormatter() {
        DateTimeFormatterBuilder builder = formatterBuilderWithoutTimezone();
        builder.optionalStart().appendLiteral(' ').optionalEnd().optionalStart().appendOffsetId().optionalEnd();

        // To handle a strange timestamp format we've seen containing two separate zone ids
        // ex. "2020-10-02 23:05:38.000000 UTC GMT"
        builder.optionalStart()
                .appendZoneText(TextStyle.SHORT)
                .appendLiteral(' ')
                .appendZoneText(TextStyle.SHORT)
                .optionalEnd();

        return builder.toFormatter();
    }

    static DateTimeFormatter localDateTimeFormatter() {
        DateTimeFormatterBuilder builder = formatterBuilderWithoutTimezone();
        return builder.toFormatter();
    }

    private void normalize() {
        switch (type) {
            case NUMBER:
                /* see https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832 */
                if (precision == null && scale == null) {
                    // if both are undefined, oracle allows basically any number, so we need to treat like a Double
                    // leave both undefined
                    break;
                }

                // if precision is undefined, but scale is defined, oracle defaults to 38
                if (precision == null) {
                    precision = DEFAULT_PRECISION;
                }
                // if precision is defined, but scale is undefined, oracle defaults to 0
                if (scale == null) {
                    scale = DEFAULT_SCALE;
                }

                if (scale < 0) {
                    // convert to bigger precision
                    precision = precision - scale;
                    scale = 0;
                }
                break;
            case FLOAT:
                precision = null;
                scale = null;

                break;
        }

        // If precision exceeds redshift max 38, then treat it as if precision were not
        // specified because then precision will be determined from the data values (most
        // likely they don't need all that precision)
        if (precision != null && precision > 38) {
            precision = null;
            scale = null;
        }
    }

    Optional<DataType> getExtractionType() {
        switch (type) {
            case NUMBER:
            case FLOAT:
                return Optional.of(DataType.BigDecimal);

            default:
                // use the value from getWarehouseType as the extraction type
                return Optional.empty();
        }
    }

    Optional<DataType> getWarehouseType() {
        switch (type) {
            case NUMBER:
                if (precision == null || scale == null) {
                    return Optional.of(DataType.Double);
                }

                if (scale == 0) {
                    // if scale is 0 and precision is defined, we have an int/long/bigint
                    if (precision < 5) {
                        // `short` has a max "length" of 5, so anything less than 5 is safely short
                        return Optional.of(DataType.Short);
                    } else if (precision < 10) {
                        // `int` has a max "length" of 10, so anything less than 10 is safely int
                        return Optional.of(DataType.Int);
                    } else if (precision < 19) {
                        // `long` has a max "length" of 18, so anything less than 18 is safely long
                        return Optional.of(DataType.Long);
                    }
                    // anything longer is technically a BigInteger, but BigDecimal is supported by core
                }
                // scale is > 0, so we need a BigDecimal for accuracy
                return Optional.of(DataType.BigDecimal);

            case FLOAT:
                // FLOAT is ingested as BigDecimal to avoid changing the value, but the warehouse type must be
                // Double
                return Optional.of(DataType.Double);

            case CHAR:
            case VARCHAR:
            case VARCHAR2:
            case NCHAR:
            case NVARCHAR2:
            case TIME:
            case TIME_WITH_TIMEZONE:
            case CLOB: // Only with HVA
            case NCLOB: // Only with HVA
                return Optional.of(DataType.String);

            case DATE:
                return Optional.of(DataType.LocalDate);
            case TIMESTAMP:
                return Optional.of(DataType.LocalDateTime);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Optional.of(DataType.Instant);

            case RAW:
            case BLOB: // Only with HVA
                return Optional.of(DataType.Binary);

            default:
                // unsupported column type
                return Optional.empty();
        }
    }

    public static OracleType create(
            String oracleColumnType,
            Integer byteLength,
            Integer precision,
            Integer scale,
            Integer columnId,
            boolean nullable) {
        return new OracleType(oracleColumnType, byteLength, precision, scale, columnId, nullable, false);
    }

    public static OracleType create(
            String oracleColumnType,
            Integer byteLength,
            Integer precision,
            Integer scale,
            Integer columnId,
            boolean nullable,
            boolean isHva) {
        return new OracleType(oracleColumnType, byteLength, precision, scale, columnId, nullable, isHva);
    }

    public static OracleType create(
            String oracleColumnType,
            BigDecimal byteLength,
            BigDecimal precision,
            BigDecimal scale,
            Integer columnId,
            boolean nullable,
            boolean isHva) {
        Integer byteLengthI = byteLength == null ? null : byteLength.intValue();
        Integer precisionI = precision == null ? null : precision.intValue();
        Integer scaleI = scale == null ? null : scale.intValue();
        return new OracleType(oracleColumnType, byteLengthI, precisionI, scaleI, columnId, nullable, isHva);
    }

    public static OracleType create(String oracleColumnType, Integer byteLength, Integer precision, Integer scale) {
        return new OracleType(oracleColumnType, byteLength, precision, scale, -1, false, false);
    }

    public static OracleType create(String oracleColumnType) {
        return new OracleType(oracleColumnType, (Integer) null, null, null, -1, false, false);
    }

    public static OracleType create(String oracleColumnType, boolean isHva) {
        return new OracleType(oracleColumnType, (Integer) null, null, null, -1, false, isHva);
    }

    public enum Type {
        LONG,
        FLOAT,
        NUMBER,
        CHAR,
        VARCHAR,
        VARCHAR2,
        NCHAR,
        NVARCHAR2,
        TIME,
        TIME_WITH_TIMEZONE,
        DATE,
        TIMESTAMP,
        TIMESTAMP_WITH_TIME_ZONE,
        TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        RAW,
        BLOB,
        CLOB,
        NCLOB,
        UNKNOWN,
        ;

        static Type fromOracleColumnType(String oracleColumnType, boolean isHva) {
            if (oracleColumnType.startsWith("TIMESTAMP")) {
                oracleColumnType = oracleColumnType.replaceAll("\\([0-9]+\\)", "");
            }

            // types marked as `alias` shouldn't actually be visible in the column definition, but keeping them here for
            // completeness and future reference
            switch (oracleColumnType.trim().toUpperCase()) {
                case "CHAR": // built-in
                case "CHARACTER": // alias
                    return CHAR;

                case "NCHAR": // built-in
                case "NATIONAL CHARACTER": // alias
                case "NATIONAL CHAR": // alias
                    return NCHAR;

                case "VARCHAR2": // built-in
                case "CHARACTER VARYING": // alias
                case "CHAR VARYING": // alias
                    return VARCHAR2;

                case "NVARCHAR2": // built-in
                case "NATIONAL CHARACTER VARYING": // alias
                case "NATIONAL CHAR VARYING": // alias
                    return NVARCHAR2;

                case "INT": // alias
                case "INTEGER": // alias
                case "SMALLINT": // alias
                    // These are aliases in Oracle, so they show up as NUMBER when we inspect the schema. There is no
                    // INTEGER type.
                case "NUMBER": // built-in
                case "NUMERIC": // alias
                case "DECIMAL": // alias
                case "DEC": // alias
                    return NUMBER;

                case "REAL": // alias for FLOAT(63)
                    // REAL is an alias in Oracle, so it shows up as FLOAT when we inspect the schema. There is no REAL
                    // type.
                case "FLOAT": // built-in
                case "DOUBLE PRECISION": // alias for FLOAT(126)
                    return FLOAT;

                case "LONG": // built-in
                    return LONG;

                case "DATE": // built-in
                    return DATE;

                case "TIMESTAMP": // built-in
                    return TIMESTAMP;

                case "TIMESTAMP WITH TIME ZONE": // built-in
                    return TIMESTAMP_WITH_TIME_ZONE;

                case "TIMESTAMP WITH LOCAL TIME ZONE": // built-in
                    return TIMESTAMP_WITH_LOCAL_TIME_ZONE;

                case "VARCHAR": // see warning here:
                    // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/Data-Types.html#GUID-DF7E10FC-A461-4325-A295-3FD4D150809E
                    return VARCHAR;

                // I can't find a record for why these are in here. I can't find anything about a "TIME" type.
                case "TIME":
                    return TIME;
                case "TIME_WITH_TIMEZONE":
                    return TIME_WITH_TIMEZONE;
                case "RAW": // built-in
                    return RAW;

                case "CLOB": // built-in
                    return isHva ? CLOB : UNKNOWN;
                case "NCLOB": // built-in
                    return isHva ? NCLOB : UNKNOWN;
                case "BLOB": // built-in
                    return isHva ? BLOB : UNKNOWN;

                case "BINARY_FLOAT": // built-in
                case "BINARY_DOUBLE": // built-in
                case "INTERVAL YEAR TO MONTH": // built-in
                case "INTERVAL DAY TO SECOND": // built-in
                case "LONG RAW": // built-in
                case "ROWID": // built-in
                case "UROWID": // built-in
                case "BFILE": // built-in
                default:
                    return UNKNOWN;
            }
        }
    }
}