package com.example.ibf.schema;

import com.example.core.TableRef;
import java.util.Map;

public class IbfTableInfo {
    public TableRef tableRef;
    public Map<String, IbfColumnInfo> columns;

    public IbfTableInfo(TableRef tableRef, Map<String, IbfColumnInfo> columns) {
        this.tableRef = tableRef;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "{tableRef=" + tableRef + ", columns=" + columns + "}";
    }

    @Override
    public boolean equals(Object o) {
        return (this == o)
                || (o != null
                        && this.getClass() == o.getClass()
                        && this.tableRef.equals(((IbfTableInfo) o).tableRef)
                        && this.columns.equals(((IbfTableInfo) o).columns));
    }

    @Override
    public int hashCode() {
        int result = tableRef.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }
}
