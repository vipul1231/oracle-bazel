package com.example.db;

import com.example.core.Column;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;

import java.util.Optional;
import java.util.OptionalInt;

/** Contains metadata for columns with supported types. */
public abstract class DbColumnInfo<SourceType> {

    public final TableRef sourceTableRef;
    public final String columnName;
    public final int ordinalPosition;
    public final SourceType sourceType;
    public DataType destinationType;
    public OptionalInt byteLength;
    public OptionalInt numericPrecision;
    public OptionalInt numericScale;
    public final boolean unsigned;

    public final boolean notNull;
    public final boolean isPrimaryKey;
    public final boolean isForeignKey;
    public final Optional<String> foreignKey;
    public final boolean excluded;

    public DbColumnInfo(
            TableRef sourceTableRef,
            String columnName,
            int ordinalPosition,
            SourceType sourceType,
            DataType destinationType,
            OptionalInt byteLength,
            OptionalInt numericPrecision,
            OptionalInt numericScale,
            boolean unsigned,
            boolean notNull,
            boolean isPrimaryKey,
            boolean isForeignKey,
            Optional<String> foreignKey,
            boolean excluded) {

        this.sourceTableRef = sourceTableRef;
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
        this.sourceType = sourceType;
        this.destinationType = destinationType;
        this.byteLength = byteLength;
        this.numericPrecision = numericPrecision;
        this.numericScale = numericScale;
        this.unsigned = unsigned;
        this.notNull = notNull;
        this.isPrimaryKey = isPrimaryKey;
        this.isForeignKey = isForeignKey;
        this.foreignKey = foreignKey;
        this.excluded = excluded;
    }

    /** @return position in ORDER BY clause for the query. */
    public int orderByQueryPosition() {
        return ordinalPosition;
    }

    public Column asExampleColumn() {
        return new Column.Builder(columnName, destinationType)
                .primaryKey(isPrimaryKey)
                .foreignKey(foreignKey)
                .byteLength(byteLength)
                .precision(numericPrecision)
                .scale(numericScale)
                .build();
    }
}
