package com.example.snowflakecritic;

import com.example.core.TableRef;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SnowflakeSQLUtils {
    private SnowflakeSQLUtils() {};

    public static String whereIn(List<String> columnNames, Collection<List<Object>> values) {
        StringBuilder clauseBuilder = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = uppercaseAndQuote(columnNames.get(i));

            clauseBuilder.append(columnName).append(" IN (");
            int curIndex = i;
            Set<Object> singleValueSet = values.stream().map(v -> v.get(curIndex)).collect(Collectors.toSet());
            clauseBuilder.append(
                    singleValueSet
                            .stream()
                            .map(value -> value instanceof String ? escape((String) value) : value.toString())
                            .collect(Collectors.joining(", ")));
            clauseBuilder.append(")");
            if (i + 1 != columnNames.size()) clauseBuilder.append(" AND ");
        }
        return clauseBuilder.toString();
    }

    public static String quote(TableRef table) {
        return uppercaseAndQuote(table.schema) + "." + uppercaseAndQuote(table.name);
    }

    private static String uppercaseAndQuote(String symbol) {
        return '"' + symbol.toUpperCase().replace("\"", "\"\"") + '"';
    }

    static String escape(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    public static String select(Set<SnowflakeColumnInfo> columns, String from) {
        return "SELECT "
                + String.join(
                ",",
                columns.stream()
                        .map(c -> SnowflakeSQLUtils.escape(c.columnName))
                        .sorted()
                        .collect(Collectors.toList()))
                + " FROM "
                + from;
    }
}
