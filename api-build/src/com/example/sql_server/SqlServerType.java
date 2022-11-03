package com.example.sql_server;

import com.example.core.annotations.DataType;
import java.util.Optional;

enum SqlServerType {
    BIGINT(true),
    BOOLEAN(true),
    BIT(true),
    CHAR(true),
    NCHAR(true),
    TEXT(true),
    NTEXT(true),
    VARCHAR(true),
    NVARCHAR(true),
    UNIQUEIDENTIFIER(true),
    XML(true),
    TIME(true),
    HIERARCHYID(true),
    DATE(true),
    REAL(true),
    FLOAT(true),
    INT(true),
    DECIMAL(true),
    NUMERIC(true),
    MONEY(true),
    SMALLMONEY(true),
    TINYINT(true),
    SMALLINT(true),
    DATETIME(true),
    SMALLDATETIME(true),
    DATETIME2(true),
    DATETIMEOFFSET(true),
    BINARY(true),
    VARBINARY(true),
    IMAGE(true),
    TIMESTAMP(true),
    ROWVERSION(true),
    GEOMETRY(true),
    GEOGRAPHY(true),

    SQL_VARIANT(false),
    TABLE(false),
    CURSOR(false);

    final boolean isSupported;

    SqlServerType(boolean isSupported) {
        this.isSupported = isSupported;
    }

    static Optional<DataType> destinationTypeOf(SqlServerType colType) {
        switch (colType) {
            case BIGINT:
                return Optional.of(DataType.Long);

            case BOOLEAN:
            case BIT:
                return Optional.of(DataType.Boolean);

            case CHAR:
            case NCHAR:
            case TEXT:
            case NTEXT:
            case VARCHAR:
            case NVARCHAR:
            case UNIQUEIDENTIFIER:
            case XML:
            case TIME:
            case HIERARCHYID:
                return Optional.of(DataType.String);

            case DATE:
                return Optional.of(DataType.LocalDate);

            case REAL:
                return Optional.of(DataType.Float);
            case FLOAT:
                return Optional.of(DataType.Double);

            case INT:
                return Optional.of(DataType.Int);

            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return Optional.of(DataType.BigDecimal);

            case TINYINT:
            case SMALLINT:
                return Optional.of(DataType.Short);

            case DATETIME:
            case SMALLDATETIME:
            case DATETIME2:
                return Optional.of(DataType.LocalDateTime);

            case DATETIMEOFFSET:
                return Optional.of(DataType.Instant);

            case BINARY:
            case VARBINARY:
            case IMAGE:
            case TIMESTAMP:
            case ROWVERSION:
                return Optional.of(DataType.Binary);

            case GEOMETRY:
            case GEOGRAPHY:
                return Optional.of(DataType.Json);

            case SQL_VARIANT:
            case TABLE:
            case CURSOR:
            default:
                return Optional.empty();
        }
    }
}
