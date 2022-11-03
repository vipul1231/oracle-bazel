package com.example.oracle;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleLogMinerValueConverter {
    private static final Pattern PATTERN_TO_TIMESTAMP =
            Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_TO_TIMESTAMP_TZ =
            Pattern.compile("TO_TIMESTAMP_TZ\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_TO_DATE =
            Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yyyy")
                    .optionalStart()
                    .appendPattern(" HH.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yyyy HH.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalStart()
                    .appendPattern(" ") // Optional: sometimes there is no space between time and zoneid
                    .optionalEnd()
                    .optionalStart()
                    .appendZoneId() // optional: if not provided then we'll assume UTC
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter AGENT_TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd")
                    .optionalStart()
                    .appendPattern(" HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter AGENT_TIMESTAMP_TZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalStart()
                    .appendPattern(" ")
                    .optionalEnd()
                    .appendZoneId()
                    .toFormatter();

    private class TimestampParser {
        private final DateTimeFormatter formatter;
        private final TemporalAccessor defaultValue;

        TimestampParser(DateTimeFormatter formatter, TemporalAccessor defaultValue) {
            this.formatter = formatter;
            this.defaultValue = defaultValue;
        }

        TemporalAccessor parse(String expresssion) throws DateTimeException {
            try {
                return formatter.parse(expresssion);
            } catch (DateTimeException ex) {
                if (defaultValue != null) {
                    ++invalidTimestampCounter;
                    return defaultValue;
                }

                throw ex;
            }
        }
    }

    private static final TemporalAccessor DEFAULT_MIN_TIMESTAMP_TZ =
            TIMESTAMP_TZ_FORMATTER.parse("01-JAN-0001 00.00.00.00 +00:00");
    private static final TemporalAccessor DEFAULT_MIN_TIMESTAMP = TIMESTAMP_FORMATTER.parse("01-JAN-0001 00.00.00.00");

    private final TimestampParser timestampParser;
    private final TimestampParser timestampTzParser;

    // If OracleTimestampParsingErrorHackfix is enabled then we'll keep track of how many
    // times an invalid timestamp value was substituted with the default.
    private int invalidTimestampCounter = 0;

    public OracleLogMinerValueConverter() {
        this.timestampParser =
                new TimestampParser(
                        TIMESTAMP_FORMATTER,
                        DEFAULT_MIN_TIMESTAMP);

        this.timestampTzParser =
                new TimestampParser(
                        TIMESTAMP_TZ_FORMATTER,
                        DEFAULT_MIN_TIMESTAMP_TZ);
    }

    public TemporalAccessor convertStringToDateTime(String val) throws Exception {
        Matcher matcher = PATTERN_TO_TIMESTAMP.matcher(val);
        if (matcher.matches()) {
            return LocalDateTime.from(timestampParser.parse(matcher.group(1).trim()));
        }

        matcher = PATTERN_TO_TIMESTAMP_TZ.matcher(val);
        if (matcher.matches()) {
            TemporalAccessor timeObj = timestampTzParser.parse(matcher.group(1).trim());
            try {
                return ZonedDateTime.from(timeObj).toInstant();
            } catch (DateTimeException e) {
                // If the time object does not have a timezone offset then convert it from
                // a LocalDateTime to a ZonedDateTime assuming it is UTC
                return LocalDateTime.from(timeObj).atZone(ZoneId.of("UTC")).toInstant();
            }
        }

        matcher = PATTERN_TO_DATE.matcher(val);
        if (matcher.matches()) {
            TemporalAccessor timeObj = timestampParser.parse(matcher.group(1).trim());
            try {
                return LocalDateTime.from(timeObj);
            } catch (DateTimeException e) {
                return LocalDate.from(timeObj);
            }
        }

        throw new Exception("Cannot parse timestamp/date: value '" + val + "'");
    }

    public int getInvalidTimestampCounter() {
        return invalidTimestampCounter;
    }

    static Object convertAgentStringToDateTime(String val) {
        return LocalDateTime.from(AGENT_TIMESTAMP_FORMATTER.parse(val.trim()));
    }

    static Object convertAgentStringToDateTimeZone(String val) {
        return ZonedDateTime.from(AGENT_TIMESTAMP_TZ_FORMATTER.parse(val.trim())).toInstant();
    }

    public static byte[] stringToByteArray(String strVal) {
        final String hexToRaw = "HEXTORAW('";
        final int hexToRawLen = hexToRaw.length();
        if (strVal.startsWith(hexToRaw)) {
            strVal = strVal.substring(hexToRawLen, strVal.length() - 2);
            return hexStringToByteArray(strVal);
        } else {
            return strVal.getBytes();
        }
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
