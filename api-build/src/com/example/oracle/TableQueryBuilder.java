package com.example.oracle;

import java.util.List;
import java.util.stream.Collectors;

import static com.example.oracle.Constants.ROW_ID_COLUMN_NAME;

public class TableQueryBuilder {
    private final OracleSqlExpressionBuilder expressionBuilder = new OracleSqlExpressionBuilder();

    protected final OracleTableInfo tableInfo;

    public TableQueryBuilder(OracleTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public String createSelectRowsQuery(List<List<Object>> upsertKeyValues) {
        String sourceTable = expressionBuilder.quoteIdentifier(tableInfo.getTableRef());
        String columnsToSelect = expressionBuilder.columnsToSelect(tableInfo.getIncomingColumns());

        List<Object> singlePkValues =
                upsertKeyValues.stream().map(objects -> objects.get(0)).collect(Collectors.toList());

        // Primary keys are sorted by name
        String wherePrimaryKeyInUpserts =
                expressionBuilder.inExpression(tableInfo.getPrimaryKeys().get(0), singlePkValues);

        return "SELECT "
                + columnsToSelect
                + ", ROWID as "
                + ROW_ID_COLUMN_NAME
                + " FROM "
                + sourceTable
                + " WHERE "
                + wherePrimaryKeyInUpserts;
    }
}
