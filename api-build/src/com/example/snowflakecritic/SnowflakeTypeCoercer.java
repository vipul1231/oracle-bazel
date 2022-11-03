package com.example.snowflakecritic;

import com.example.core.annotations.DataType;
import com.example.oracle.Pair;

import java.math.BigDecimal;

public class SnowflakeTypeCoercer {

    /**
     * @param value
     * @return
     */
    Pair<Object, DataType> inferTypeAndCoerce(BigDecimal value) {

        DataType inferredDataType = SnowflakeType.inferNumberType(value.precision(), value.scale());
        switch (inferredDataType) {
            case String:
                return new Pair<>(value.stripTrailingZeros().toPlainString(), DataType.String);

            case Double:
                return new Pair<>(value.doubleValue(), DataType.Double);

            case Float:
                return new Pair<>(value.floatValue(), DataType.Float);

            case Long:
                return new Pair<>(value.longValueExact(), DataType.Long);

            case Int:
                return new Pair<>(value.intValueExact(), DataType.Int);

            case BigDecimal:
            default:
                return new Pair<>(value, DataType.BigDecimal);
        }
    }

    /**
     * @param columnInfo with a source type that is a number
     * @param value
     * @return
     */
    private Pair<Object, Boolean> promoteOrMinimizeNumber(SnowflakeColumnInfo columnInfo, BigDecimal value) {
        Pair<Object, DataType> coercedValue = inferTypeAndCoerce(value);

        if (columnInfo.destinationType != DataType.String
                && (coercedValue.right.ordinal() > columnInfo.destinationType.ordinal()
                || coercedValue.right == DataType.String)) {
            // We have a promotion
            columnInfo.destinationType = coercedValue.right;
            return new Pair<>(coercedValue.left, true);
        }

        // just a minimize
        return new Pair<>(coercedValue.left, false);
    }

    /**
     * @param column
     * @param value
     * @return
     */
    public Pair<Object, Boolean> promoteOrMinimize(SnowflakeColumnInfo column, Object value) {
        switch (column.sourceType) {
            case NUMBER:
            case FLOAT:
                return promoteOrMinimizeNumber(column, (BigDecimal) value);
            default:
                return new Pair<>(value, false);
        }
    }

    /**
     * Return the "minimal" representation of the number
     *
     * @param value
     * @return
     */
    public Object minimize(Object value, SnowflakeType dataType) {
        // future minimize logic will go here, for now just return the original value
        return value;
    }

    public Object minimizeNumber(BigDecimal number) {
        return inferTypeAndCoerce(number).left;
    }
}