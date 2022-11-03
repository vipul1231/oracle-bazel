package com.example.oracle;

import com.example.core.TableRef;
import com.example.db.SqlStatementUtils;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OracleSqlExpressionBuilder {
    public static final int ORACLE_MAX_EXPRESSION_LIST_SIZE = 1000;

    /** Add more options as needed */
    public enum Option {
        CAST_DATETIME_TO_STRING
    }

    private boolean castDateTimeToString;

    public OracleSqlExpressionBuilder(Option... options) {
        for (Option option : options) {
            if (option == Option.CAST_DATETIME_TO_STRING) {
                castDateTimeToString = true;
            }
        }
    }

    /**
     * @param columnInfo
     * @param values
     * @return
     */
    public String inExpression(OracleColumnInfo columnInfo, List<Object> values) {
        return inExpression(castColumnToTargetType(columnInfo), values);
    }

    /**
     * @param columnName
     * @param values
     * @return
     */
    public String inExpression(String columnName, Collection<?> values) {
        if (values.isEmpty()) {
            return "";
        }

        if (values.size() <= ORACLE_MAX_EXPRESSION_LIST_SIZE) {
            return innerInExpression(columnName, values);
        }

        final AtomicInteger counter = new AtomicInteger();
        List<? extends List<?>> listOfValuesLists =
                new ArrayList<>(
                        values.stream()
                                .collect(
                                        Collectors.groupingBy(
                                                it -> counter.getAndIncrement() / ORACLE_MAX_EXPRESSION_LIST_SIZE))
                                .values());

        StringBuilder clauseBuilder = new StringBuilder();
        clauseBuilder.append("(");

        for (int i = 0; i < listOfValuesLists.size(); ++i) {
            if (i > 0) {
                clauseBuilder.append(" OR ");
            }

            clauseBuilder.append(innerInExpression(columnName, listOfValuesLists.get(i)));
        }

        clauseBuilder.append(")");
        return clauseBuilder.toString();
    }

    private String innerInExpression(String columnName, Collection<?> values) {
        StringBuilder clauseBuilder = new StringBuilder();
        clauseBuilder.append(columnName).append(" IN (");
        clauseBuilder.append(values.stream().map(value -> escapeValue(value)).collect(Collectors.joining(",")));
        clauseBuilder.append(")");
        return clauseBuilder.toString();
    }

    public String quoteIdentifier(OracleColumnInfo columnInfo) {
        return "\"" + columnInfo.getName() + "\"";
    }

    public String quoteIdentifier(TableRef table) {
        return SqlStatementUtils.ORACLE.quote(table);
    }

    public String castColumnToTargetType(OracleColumnInfo columnInfo) {
        switch (columnInfo.getTargetColumn().type) {
            default:
                return quoteIdentifier(columnInfo);
        }
    }

    public String escapeValue(Object value) {
        if (value instanceof String) {
            return SqlStatementUtils.ORACLE.escapeStringValue(value.toString());
        }

        return value.toString();
    }

    private String castTimeColumnAsChar(OracleColumnInfo columnInfo) {
        return "TO_CHAR(" + quoteIdentifier(columnInfo) + ") AS " + quoteIdentifier(columnInfo);
    }

    public String prepareColumnForSelect(OracleColumnInfo columnInfo) {
        return (castDateTimeToString && columnInfo.getSourceColumn().oracleType.isDateTimeLike())
                ? castTimeColumnAsChar(columnInfo)
                : quoteIdentifier(columnInfo);
    }

    /**
     * @param sourceColumnInfo
     * @return a comma-separate list of column identifiers, properly quoted and cast as needed
     */
    public String columnsToSelect(Collection<OracleColumnInfo> sourceColumnInfo) {
        List<String> columnsToSelect =
                sourceColumnInfo
                        .stream()
                        .map(columnInfo -> prepareColumnForSelect(columnInfo))
                        .collect(Collectors.toList());

        return Joiner.on(", ").join(columnsToSelect);
    }
}
