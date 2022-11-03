package com.example.oracle;

import com.example.core.TableRef;
import com.example.ibf.schema.IbfColumnInfo;
import com.example.ibf.schema.IbfTableInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Provides all table related information required by the ibf incremental updater. */
public class OracleIbfTableInfo {
    private final IbfTableInfo ibfTableInfo;
    private Map<String, IbfColumnInfo> modifiedColumns = new HashMap<>();
    private final OracleTableInfo oracleTableInfo;

    public OracleIbfTableInfo(OracleTableInfo oracleTableInfo) {
        this.oracleTableInfo = oracleTableInfo;
        this.ibfTableInfo = newIbfTableInfo(oracleTableInfo);
    }

    public OracleIbfTableInfo setModifiedColumns(Map<String, IbfColumnInfo> modifiedColumns) {
        this.modifiedColumns = modifiedColumns;

        for (IbfColumnInfo ibfColumnInfo : modifiedColumns.values()) {
            Objects.requireNonNull(
                            oracleTableInfo.getColumnInfo(ibfColumnInfo.columnName),
                            "Column "
                                    + ibfColumnInfo.columnName
                                    + " not present in "
                                    + oracleTableInfo.getIncomingColumns())
                    .setAddedSinceLastSync(true);
        }

        return this;
    }

    public Map<String, IbfColumnInfo> getModifiedColumns() {
        return modifiedColumns;
    }

    public TableRef getTableRef() {
        return ibfTableInfo.tableRef;
    }

    public IbfTableInfo getIbfTableInfo() {
        return ibfTableInfo;
    }

    public OracleTableInfo getOracleTableInfo() {
        return oracleTableInfo;
    }

    public static IbfTableInfo newIbfTableInfo(OracleTableInfo oracleTableInfo) {
        Map<String, IbfColumnInfo> columns =
                oracleTableInfo
                        .getIncomingColumns()
                        .stream()
                        .collect(Collectors.toMap(c -> c.getName(), c -> new OracleIbfColumnInfo(c)));
        return new IbfTableInfo(oracleTableInfo.getTableRef(), columns);
    }
}
