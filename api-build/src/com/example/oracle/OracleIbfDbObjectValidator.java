package com.example.oracle;

import java.util.*;

import static com.example.oracle.OracleType.Type.*;

public class OracleIbfDbObjectValidator extends OracleDbObjectValidator {

    private static final Set<OracleType.Type> SUPPORTED_PK_TYPES =
            new HashSet<>(Arrays.asList(NUMBER, VARCHAR, VARCHAR2, RAW));

    private static final Set<OracleType.Type> SUPPORTED_COLUMN_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            NUMBER,
                            VARCHAR,
                            VARCHAR2,
                            RAW,
                            CHAR,
                            NCHAR,
                            NVARCHAR2,
                            FLOAT,
                            TIME,
                            TIME_WITH_TIMEZONE,
                            DATE,
                            TIMESTAMP,
                            TIMESTAMP_WITH_TIME_ZONE,
                            TIMESTAMP_WITH_LOCAL_TIME_ZONE));

    @Override
    public void validate(OracleTableInfo tableInfo) throws Exception {
        if (tableInfo.getPrimaryKeys().isEmpty()) {
            throw new Exception(tableInfo.getTableRef() + " does not have a primary key");
        }

        List<String> unsupportedPkColumns = new ArrayList<>();

        for (OracleColumnInfo pkColInfo : tableInfo.getPrimaryKeys()) {
            if (!primaryKeyTypeSupported(pkColInfo.getOracleType())) {
                unsupportedPkColumns.add(pkColInfo.getName() + ":" + pkColInfo.getOracleType().type);
            }
        }

        if (!unsupportedPkColumns.isEmpty()) {
            throw new Exception(
                    "Unsupported type(s) in primary key column(s) "
                            + String.join(",", unsupportedPkColumns)
                            + " in table "
                            + tableInfo.getTableRef());
        }
    }

    @Override
    public boolean columnTypeSupported(OracleType type) {
        // If the destination supports the type...
        if (super.columnTypeSupported(type)) {
            return SUPPORTED_COLUMN_TYPES.contains(type.type);
        }

        return false;
    }

    @Override
    public boolean primaryKeyTypeSupported(OracleType type) {
        return SUPPORTED_PK_TYPES.contains(type.type);
    }
}
