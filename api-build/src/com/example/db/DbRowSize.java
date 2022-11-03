package com.example.db;

import com.example.logger.ExampleLogger;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DbRowSize {
    private static ExampleLogger LOG = ExampleLogger.getMainLogger();
    private static Set<Class> warnedClasses = new HashSet<>();
    /**
     * These numbers were arrived at by inspecting the underling classes and understanding their members. If you think
     * any of these are wrong, you might be right. It's not the gospel truth.
     *
     * @param map - A map of simple values (no nesting!)
     * @return the number of bytes that the values in the map take up in memory
     */
    public static long mapValues(Map<?, ?> map) {
        long sum = 0;
        for (Map.Entry<?, ?> e : map.entrySet()) {
            Object o = e.getValue();
            sum += value(o);
        }
        return sum;
    }

    public static long listValues(List<?> list) {
        long sum = 0;
        for (Object o : list) {
            sum += value(o);
        }
        return sum;
    }

    private static long value(Object o) {
        if (o == null) return 0;
        else if (o.getClass() == Short.class) return 2;
        else if (o.getClass() == Integer.class) return 4;
        else if (o.getClass() == Long.class) return 8;
        else if (o.getClass() == Float.class) return 4;
        else if (o.getClass() == Double.class) return 8;
        else if (o.getClass() == String.class) return ((String) o).length() * 2; // UTF size of code point, in bytes
        else if (o.getClass() == Boolean.class) return 1;
        else if (o.getClass() == BigDecimal.class) return (4 + 4 + 8 + 4 + 8);
        // (BigDecimal scale, int precision, long intCompact) + (BigInteger int signum +int[] mag,
        // treating mag as 8 because we can't see it);
        else if (o.getClass() == Instant.class) return (8 + 4); // Long seconds + Int nanos
        else if (o.getClass() == LocalDateTime.class) return ((4 + 2 + 2) + (1 + 1 + 1 + 4));
        // (Integer year + short month + short day) + (byte hour + byte minute + byte second + int nanos)
        else if (o.getClass() == LocalDate.class) return (4 + 2 + 2);
        else if (o instanceof JsonNode)
            return o.toString().length(); // instanceof here because nodes could be one of many types of JsonNode
        else if (o instanceof byte[]) return ((byte[]) o).length;
        else {
            if (!warnedClasses.contains(o.getClass())) {
                LOG.warning("Cannot determine size of data for " + o.getClass().toGenericString());
                warnedClasses.add(o.getClass());
            }

            return 0;
        }
    }
}
