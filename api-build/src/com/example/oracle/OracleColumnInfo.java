package com.example.oracle;

import com.example.core.Column;
import com.example.core.annotations.DataType;

import java.math.BigDecimal;
import java.util.Optional;

public class OracleColumnInfo {
    private final OracleColumn sourceColumn;
    private final Column targetColumn;

    private Optional<Object> defaultValue = Optional.empty();

    /**
     * a value of true means this is a newly added column: it wasn't present in the previous sync but was added before
     * this current sync.
     */
    private boolean addedSinceLastSync;

    public OracleColumnInfo(OracleColumn oracleColumn, Column column) {
        this.sourceColumn = oracleColumn;
        this.targetColumn = column;
    }

    public Column getTargetColumn() {
        return targetColumn;
    }

    public OracleColumn getSourceColumn() {
        return sourceColumn;
    }

    public OracleType.Type getType() {
        return sourceColumn.oracleType.type;
    }

    public OracleType getOracleType() {
        return sourceColumn.oracleType;
    }

    public String getName() {
        return sourceColumn.name;
    }

    public DataType getDestinationType() {
        return targetColumn.type;
    }

    public boolean isPrimaryKey() {
        return sourceColumn.primaryKey;
    }

    public Optional<Integer> getLength() {
        return Optional.ofNullable(sourceColumn.oracleType.getRawByteLength());
    }

    public Optional<String> getColumnDefaultValue() {
        return sourceColumn.getDataDefault();
    }

    public Optional<Integer> getKeyLength() {
        return getLength();
    }

    public boolean isAddedSinceLastSync() {
        return addedSinceLastSync;
    }

    public void setAddedSinceLastSync(boolean addedSinceLastSync) {
        this.addedSinceLastSync = addedSinceLastSync;
    }

    public String getSchemaName() {
        return sourceColumn.getSourceTable().schema;
    }

    public String getTableName() {
        return sourceColumn.getSourceTable().name;
    }

    /**
     * Sets the default column value iff the expression is parseable and not empty.
     *
     * @param expression may be a PL/SQL expression or a literal value.
     * @return the oracle column info
     */
    public OracleColumnInfo parseAndSetDataDefaultExpression() {
        Optional<String> dataDefault = sourceColumn.getDataDefault();
        Optional<Object> dataDefaultParsed = parseDataDefaultExpression(dataDefault);
        if (dataDefaultParsed.isPresent()) {
            defaultValue = dataDefaultParsed;
        }

        return this;
    }

    public Optional<Object> parseDataDefaultExpression(Optional<String> expression) {
        if (expression.isPresent()) {
            String expressionStr = expression.get();

            if (!"NULL".equals(expressionStr) && !expressionStr.isEmpty()) {
                if (getOracleType().isStringLike()) {
                    if (expressionStr.startsWith("'") && expressionStr.endsWith("'")) {
                        return Optional.of(expressionStr.substring(1, expressionStr.length() - 1));
                    }
                } else if (getOracleType().isNumber()) {
                    try {
                        return Optional.of(new BigDecimal(expressionStr));
                    } catch (Exception ignored) {
                        // Not parsable
                    }
                }
            }
        }

        return Optional.empty();
    }

    public Optional<Object> getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "OracleColumnInfo{" + getSchemaName() + "." + getTableName() + "." + sourceColumn.name + "}";
    }
}
