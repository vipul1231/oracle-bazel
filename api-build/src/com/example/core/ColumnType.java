package com.example.core;


import com.example.core.annotations.DataType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;
import java.util.OptionalInt;

/** The type parts of Column, without the uniqueness and distribution properties */
public class ColumnType implements Serializable {
    public static final ColumnType UNKNOWN = ColumnType.fromType(DataType.Unknown, false);

    public final DataType type;
    public final OptionalInt byteLength;
    public final OptionalInt precision;
    public final OptionalInt scale;
    public final boolean notNull;

    @JsonCreator
    public ColumnType(
            @JsonProperty("type") DataType type,
            @JsonProperty("byteLength") OptionalInt byteLength,
            @JsonProperty("precision") OptionalInt precision,
            @JsonProperty("scale") OptionalInt scale,
            @JsonProperty("notNull") boolean notNull) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(byteLength);
        Objects.requireNonNull(precision);
        Objects.requireNonNull(scale);

        if (byteLength.isPresent() && type != DataType.String && type != DataType.Json && type != DataType.Binary)
            byteLength = OptionalInt.empty();

        if (precision.isPresent() && type != DataType.BigDecimal) precision = OptionalInt.empty();

        if (scale.isPresent() && type != DataType.BigDecimal) scale = OptionalInt.empty();

        this.type = type;
        this.byteLength = byteLength;
        this.precision = precision;
        this.scale = scale;
        this.notNull = notNull;
    }

    public static ColumnType decimal(boolean notNull) {
        return new ColumnType(
                DataType.BigDecimal, OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public static final int SHORT_PRECISION = (int) Math.ceil(Math.log10(Short.MAX_VALUE));
    public static final int INT_PRECISION = (int) Math.ceil(Math.log10(Integer.MAX_VALUE));
    public static final int LONG_PRECISION = (int) Math.ceil(Math.log10(Long.MAX_VALUE));

    private static int effectivePrecision(ColumnType column) {
        switch (column.type) {
            case Short:
                return SHORT_PRECISION;
            case Int:
                return INT_PRECISION;
            case Long:
                return LONG_PRECISION;
            case BigDecimal:
                return column.precision.orElse(Column.DEFAULT_PRECISION);
            default:
                return -1;
        }
    }

    @Override
    public String toString() {
        StringBuilder acc = new StringBuilder();

        acc.append(type.toString());

        if (byteLength.isPresent()) acc.append("(").append(byteLength.getAsInt()).append(")");

        if (precision.isPresent() || scale.isPresent())
            acc.append("(").append(precision.orElse(-1)).append(", ").append(scale.orElse(-1)).append(")");

        if (notNull) acc.append(" NOT NULL");

        return acc.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnType that = (ColumnType) o;
        return notNull == that.notNull
                && type == that.type
                && Objects.equals(byteLength, that.byteLength)
                && Objects.equals(precision, that.precision)
                && Objects.equals(scale, that.scale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, byteLength, precision, scale, notNull);
    }

    public static ColumnType json(OptionalInt byteLength, boolean notNull) {
        return new ColumnType(DataType.Json, byteLength, OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public static ColumnType binary(OptionalInt byteLength, boolean notNull) {
        return new ColumnType(DataType.Binary, byteLength, OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public static ColumnType string(OptionalInt byteLength, boolean notNull) {
        return new ColumnType(DataType.String, byteLength, OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public static ColumnType decimal(int precision, int scale, boolean notNull) {
        return new ColumnType(
                DataType.BigDecimal, OptionalInt.empty(), OptionalInt.of(precision), OptionalInt.of(scale), notNull);
    }

    public static ColumnType bool(boolean notNull) {
        return new ColumnType(DataType.Boolean, OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public static ColumnType fromType(DataType type, boolean notNull) {
        return new ColumnType(type, OptionalInt.empty(), OptionalInt.empty(), OptionalInt.empty(), notNull);
    }

    public ColumnType withByteLength(OptionalInt byteLength) {
        return new ColumnType(type, byteLength, precision, scale, notNull);
    }

    public ColumnType withNotNull(boolean notNull) {
        return new ColumnType(type, byteLength, precision, scale, notNull);
    }

    public ColumnType withDataType(DataType type) {
        return new ColumnType(type, byteLength, precision, scale, notNull);
    }

}
