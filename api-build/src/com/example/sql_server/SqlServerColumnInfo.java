package com.example.sql_server;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.flag.FlagName;
import com.example.db.DbColumnInfo;
import java.util.Optional;
import java.util.OptionalInt;

class SqlServerColumnInfo extends DbColumnInfo<SqlServerType> {
    public final boolean includedInCDC;

    /**
     * PK ordinal position can be different than table ordinal position.
     *
     * <p>e.g. CREATE TABLE test (column1 int, column2 int, constraints pk_1 primary key (column2, column1));
     *
     * <p>Here, normal ordinal position are 1 and 2 for column1 and column2 respectively, but PK ordinal position are 2
     * and 1 for column1 and column2 respectively.
     *
     * <p>We should use PK ordinal position for ORDER BY.
     */
    public final int primaryKeyOrdinalPosition;

    SqlServerColumnInfo(
            TableRef sourceTableRef,
            String columnName,
            int ordinalPosition,
            int primaryKeyOrdinalPosition,
            SqlServerType sqlServerType,
            DataType destinationType,
            OptionalInt byteLength,
            OptionalInt numericPrecision,
            OptionalInt numericScale,
            boolean isForeignKey,
            boolean unsigned,
            boolean notNull,
            boolean isPrimaryKey,
            Optional<String> foreignKey,
            boolean userExcluded,
            boolean includedInCDC) {
        super(
                sourceTableRef,
                columnName,
                ordinalPosition,
                sqlServerType,
                destinationType,
                byteLength,
                numericPrecision,
                numericScale,
                unsigned,
                notNull,
                isPrimaryKey,
                isForeignKey,
                foreignKey,
                userExcluded);
        this.includedInCDC = includedInCDC;
        this.primaryKeyOrdinalPosition = primaryKeyOrdinalPosition;
    }

    boolean isSpatialType() {
        return sourceType == SqlServerType.GEOMETRY || sourceType == SqlServerType.GEOGRAPHY;
    }

    @Override
    public int orderByQueryPosition() {
        if (FlagName.SqlServerOrderByPK.check()) {
            return primaryKeyOrdinalPosition;
        }
        return super.orderByQueryPosition();
    }
}
