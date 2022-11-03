package com.example.oracle;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:49 AM<br/>
 * To change this template use File | Settings | File Templates.
 */

import com.example.oracle.exceptions.TypePromotionNeededException;

import java.time.*;
import java.util.Arrays;
import java.util.Optional;

/** Oracle Data Type 180, 181 and 231 */
public class OracleTimestamp {
    /*
     * Type 180
     * TIMESTAMP(p)
     *
     * 7 or 11 bytes
     * Same as DATE, but the extra 4 bytes are used for fractional precision. If the fractional precision is 0 OR
     * if the value of the fractional second equals 0, the 4 extra bytes will be missing.
     *
     * Example: 1980-02-20 10:46:34.123456789
     * bytes[] = 119,180,2,20,11,47,35,7,91,205,21
     * byte 1  - century (excess 100) 119 - 100 = 19
     * byte 2  - year (excess 100) 180 - 100 = 80
     * byte 3  - month = 2
     * byte 4  - day = 20
     * byte 5  - hour (excess 1) 11 - 1 = 10
     * byte 6  - minute (excess 1) 47 - 1 = 46
     * byte 7  - second (excess 1) 35 - 1 = 34
     * byte 8  - fractional second in hex 7 = 07
     * byte 9  - fractional second in hex 91 = 5B
     * byte 10 - fractional second in hex 205 = CD
     * byte 11 - fractional second in hex 12 = 15
     *         - 07 5B CD 15 = 123456789
     */

    /*
     * Type 231
     * TIMESTAMP(p) WITH LOCAL TIME ZONE
     *
     * 7 or 11 bytes
     * AFAIK same as TIMESTAMP(p). Oracle application does some timezone manipulation before returning the value.
     *
     * Example: 1980-02-20 10:46:34.123456789
     * bytes[] = 119,180,2,20,11,47,35,7,91,205,21
     * byte 1  - century (excess 100) 119 - 100 = 19
     * byte 2  - year (excess 100) 180 - 100 = 80
     * byte 3  - month = 2
     * byte 4  - day = 20
     * byte 5  - hour (excess 1) 11 - 1 = 10
     * byte 6  - minute (excess 1) 47 - 1 = 46
     * byte 7  - second (excess 1) 35 - 1 = 34
     * byte 8  - fractional second in hex 7 = 07
     * byte 9  - fractional second in hex 91 = 5B
     * byte 10 - fractional second in hex 205 = CD
     * byte 11 - fractional second in hex 12 = 15
     *         - 07 5B CD 15 = 123456789
     */

    /*
     * Type 181
     * TIMESTAMP(p) WITH TIME ZONE
     *
     * Since Oracle stores the timestamp as UTC internally, we don't actually need or care about the offset. We'd only
     * need the offset if we wanted to convert the UTC time back to the local zone time. Since our goal is to get
     * everything into UTC, we just skip that.
     *
     * Fixed at 13 bytes
     * 11 bytes same as TIMESTAMP(p) with 2 more bytes for timezone offset
     *
     * Example: 1980-02-20 10:46:34.123456789 +01:00
     * bytes[] = 119,180,2,20,11,47,35,7,91,205,21,21,60
     * byte 1  - century (excess 100) 119 - 100 = 19
     * byte 2  - year (excess 100) 180 - 100 = 80
     * byte 3  - month = 2
     * byte 4  - day = 20
     * byte 5  - hour (excess 1) 11 - 1 = 10
     * byte 6  - minute (excess 1) 47 - 1 = 46
     * byte 7  - second (excess 1) 35 - 1 = 34
     * byte 8  - fractional second in hex 7 = 07
     * byte 9  - fractional second in hex 91 = 5B
     * byte 10 - fractional second in hex 205 = CD
     * byte 11 - fractional second in hex 12 = 15
     *         - 07 5B CD 15 = 123456789
     * byte 12 - zone offset hour (excess 20) 21 - 20 = 1
     * byte 13 - zone offset minute (excess 60) 60 - 60 = 0
     */

    private final int century;
    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final int second;
    private final int nanos;

    static Optional<OracleTimestamp> of(byte[] bytes) {
        if (bytes.length == 7) {
            // Type 180 or 231
            return Optional.of(
                    new OracleTimestamp(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6]));
        } else if (bytes.length == 11) {
            // Type 180 or 231
            return Optional.of(
                    new OracleTimestamp(
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                            bytes[9], bytes[10]));
        } else if (bytes.length == 13) {
            // Type 181
            return Optional.of(
                    new OracleTimestamp(
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                            bytes[9], bytes[10])); // ignore the last two bytes
        }
        return Optional.empty();
    }

    OracleTimestamp(byte century, byte year, byte month, byte day, byte hour, byte minute, byte second) {
        this.century = century & 0xff;
        this.year = year & 0xff;
        this.month = month & 0xff;
        this.day = day & 0xff;
        this.hour = hour & 0xff;
        this.minute = minute & 0xff;
        this.second = second & 0xff;
        this.nanos = 0;
    }

    OracleTimestamp(
            byte century,
            byte year,
            byte month,
            byte day,
            byte hour,
            byte minute,
            byte second,
            byte f1,
            byte f2,
            byte f3,
            byte f4) {
        this.century = century & 0xff;
        this.year = year & 0xff;
        this.month = month & 0xff;
        this.day = day & 0xff;
        this.hour = hour & 0xff;
        this.minute = minute & 0xff;
        this.second = second & 0xff;
        this.nanos = createNanos(f1, f2, f3, f4);
    }

    private int createNanos(byte f1, byte f2, byte f3, byte f4) {
//        String hex = Strings.padStart(Integer.toHexString(f1 & 0xff), 2, '0');
//        hex += Strings.padStart(Integer.toHexString(f2 & 0xff), 2, '0');
//        hex += Strings.padStart(Integer.toHexString(f3 & 0xff), 2, '0');
//        hex += Strings.padStart(Integer.toHexString(f4 & 0xff), 2, '0');
//        long nanosL = Long.parseLong(hex, 16);
        long nanosL = -1L;
        if (nanosL > 999999999L || nanosL < 0L) {
            throw new DateTimeException("Nanos outside of range 0 - 999999999");
        }
        return (int) nanosL;
    }

    int getYear() {
        return ((century - 100) * 100) + (year - 100);
    }

    int getMonth() {
        return month;
    }

    int getDay() {
        return day;
    }

    int getHour() {
        return hour - 1;
    }

    int getMinute() {
        return minute - 1;
    }

    int getSecond() {
        return second - 1;
    }

    int getNanos() {
        return nanos;
    }

    Instant toInstant() throws DateTimeException {
        return toLocalDateTime().toInstant(ZoneOffset.UTC);
    }

    LocalDateTime toLocalDateTime() throws DateTimeException {
        return LocalDateTime.of(getYear(), getMonth(), getDay(), getHour(), getMinute(), getSecond(), getNanos());
    }

    LocalDate toLocalDate() throws DateTimeException, TypePromotionNeededException {
        if (getHour() != 0 || getMinute() != 0 || getSecond() != 0) {
            throw new TypePromotionNeededException("Date contains time information");
        }
        return LocalDate.of(getYear(), getMonth(), getDay());
    }

    static Instant instantFromBytes(byte[] rawBytes, OracleColumn oracleColumn) {
        try {
            Optional<OracleTimestamp> maybeOracleTimestamp = of(rawBytes);
            if (maybeOracleTimestamp.isPresent()) {
                return maybeOracleTimestamp.get().toInstant();
            }
        } catch (DateTimeException e) {
            // fall down to the code below
        }
        // failure condition (Optional is empty OR DateTimeException happened
        if (oracleColumn != null) {
            oracleColumn.badDateVals.add(Arrays.toString(rawBytes));
        }
        return null;
    }

    static LocalDateTime localDateTimeFromBytes(byte[] rawBytes, OracleColumn oracleColumn) {
        try {
            Optional<OracleTimestamp> maybeOracleTimestamp = of(rawBytes);
            if (maybeOracleTimestamp.isPresent()) {
                return maybeOracleTimestamp.get().toLocalDateTime();
            }
        } catch (DateTimeException e) {
            // fall down to the code below
        }
        // failure condition (Optional is empty OR DateTimeException happened
        if (oracleColumn != null) {
            oracleColumn.badDateVals.add(Arrays.toString(rawBytes));
        }
        return null;
    }

    static LocalDate localDateFromBytes(byte[] rawBytes, OracleColumn oracleColumn)
            throws TypePromotionNeededException {
        try {
            Optional<OracleTimestamp> maybeOracleTimestamp = of(rawBytes);
            if (maybeOracleTimestamp.isPresent()) {
                return maybeOracleTimestamp.get().toLocalDate();
            }
        } catch (DateTimeException e) {
            // fall down to the code below
        }
        // failure condition (Optional is empty OR DateTimeException happened
        if (oracleColumn != null) {
            oracleColumn.badDateVals.add(Arrays.toString(rawBytes));
        }
        return null;
    }
}