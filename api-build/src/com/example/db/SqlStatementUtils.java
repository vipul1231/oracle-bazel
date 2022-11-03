package com.example.db;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.flag.FlagName;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public enum SqlStatementUtils {
    MYSQL,
    SQL_SERVER,
    DB2,
    ORACLE;

    public String quote(String identifier) {
        switch (this) {
            case SQL_SERVER:
                return '[' + identifier.replace("]", "]]") + ']';
            case MYSQL:
                return '`' + identifier.replace("`", "``") + '`';
            case DB2:
                return '"' + identifier.replace("\"", "\"\"") + '"';
            case ORACLE:
                return '"' + identifier.replace("\"", "\"\"") + '"';
        }
        return '`' + identifier.replace("`", "``") + '`';
    }

    public String quote(TableRef table) {
        return quote(table.schema) + '.' + quote(table.name);
    }

    public String castValue(DataType type, String value) {
        switch (this) {
            case SQL_SERVER:
                return (type == DataType.Binary) ? value : escapeStringValue(value);
            case MYSQL:
                return (FlagName.MysqlImportWithoutHexingBinaryPKCol.check() && type == DataType.Binary)
                        ? "X" + escapeStringValue(value)
                        : escapeStringValue(value);
            case DB2:
                return escapeStringValue(value);
        }
        return value;
    }

    /**
     * Escape any delimiter characters (single apostrophes become double apostrophes)
     *
     * @param value
     * @return
     */
    public String escapeStringValue(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    public String castColumn(DataType type, String columnName) {
        switch (this) {
            case MYSQL:
                return (!FlagName.MysqlImportWithoutHexingBinaryPKCol.check() && type == DataType.Binary)
                        ? "hex(" + columnName + ")"
                        : columnName;
            case SQL_SERVER:
            default:
                return columnName;
        }
    }

    public String columnsForOrderBy(Collection<? extends com.example.db.DbColumnInfo<?>> allKeyColumnInfo) {

        return allKeyColumnInfo
                .stream()
                .sorted(Comparator.comparingInt(com.example.db.DbColumnInfo::orderByQueryPosition))
                .map(ci -> quote(ci.columnName))
                .collect(Collectors.joining(", "));
    }

    @SuppressWarnings({"java:S2583", "java:S4274"})
    public String whereGreaterThan(List<? extends com.example.db.DbColumnInfo<?>> allKeyColumnInfo, List<String> lastEvaluatedKey) {
        int numColumns = allKeyColumnInfo.size();
        assert numColumns == lastEvaluatedKey.size() && numColumns > 0;

        StringBuilder clauseBuilder = new StringBuilder();
        for (int lastCol = 0; lastCol < numColumns; lastCol++) {
            if (lastCol > 0) clauseBuilder.append(" OR ");
            clauseBuilder.append("(");

            for (int col = 0; col <= lastCol; col++) {
                if (col > 0) clauseBuilder.append(" AND ");

                String columnName = quote(allKeyColumnInfo.get(col).columnName);
                String column = castColumn(allKeyColumnInfo.get(col).destinationType, columnName);
                String value = lastEvaluatedKey.get(col);

                assert value != null;

                clauseBuilder
                        .append(column)
                        .append((col == lastCol ? " > " : " = "))
                        .append(castValue(allKeyColumnInfo.get(col).destinationType, value));
            }
            clauseBuilder.append(")");
        }

        return clauseBuilder.toString();
    }
}
