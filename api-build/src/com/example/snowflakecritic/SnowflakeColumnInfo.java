package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.db.DbColumnInfo;

import java.util.Optional;
import java.util.OptionalInt;

public class SnowflakeColumnInfo extends DbColumnInfo<SnowflakeType> {
    public OptionalInt maxLength;
    public boolean isReplicationKey;

    public SnowflakeColumnInfo(
            TableRef sourceTableRef,
            String columnName,
            int ordinalPosition,
            SnowflakeType snowflakeColumnType,
            DataType destinationType,
            OptionalInt byteLength,
            OptionalInt numericPrecision,
            OptionalInt numericScale,
            boolean notNull,
            boolean isPrimaryKey,
            boolean isForeignKey,
            Optional<String> foreignKey,
            boolean excluded,
            boolean isReplicationKey) {
        super(
                sourceTableRef,
                columnName,
                ordinalPosition,
                snowflakeColumnType,
                destinationType,
                byteLength,
                numericPrecision,
                numericScale,
                false, // Snowflake does not have an UNSIGNED type modifier
                notNull,
                isPrimaryKey,
                isForeignKey,
                foreignKey,
                excluded);
        this.isReplicationKey = isReplicationKey;
    }

    public SnowflakeColumnInfo(
            TableRef sourceTableRef,
            String columnName,
            int ordinalPosition,
            SnowflakeType snowflakeColumnType,
            DataType destinationType,
            OptionalInt byteLength,
            OptionalInt maxLength,
            OptionalInt numericPrecision,
            OptionalInt numericScale,
            boolean notNull,
            boolean isPrimaryKey,
            boolean isForeignKey,
            Optional<String> foreignKey,
            boolean excluded,
            boolean isReplicationKey) {
        super(
                sourceTableRef,
                columnName,
                ordinalPosition,
                snowflakeColumnType,
                destinationType,
                byteLength,
                numericPrecision,
                numericScale,
                false, // Snowflake does not have an UNSIGNED type modifier
                notNull,
                isPrimaryKey,
                isForeignKey,
                foreignKey,
                excluded);
        this.maxLength = maxLength;
        this.isReplicationKey = isReplicationKey;
    }

    @Override
    public String toString() {
        return columnName;
    }

    public OptionalInt getMaxLength() {
        return maxLength;
    }
}
