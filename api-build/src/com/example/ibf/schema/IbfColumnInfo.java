package com.example.ibf.schema;

import com.example.core.annotations.DataType;

public class IbfColumnInfo {
    public String columnName;
    public DataType destColumnType;
    public Object columnDefaultValue;

    // constructor for IbfColumnInfoSerializer
    public IbfColumnInfo(String columnName, DataType destColumnType) {
        this(columnName, destColumnType, null);
    }

    public IbfColumnInfo(String columnName, DataType destColumnType, Object columnDefaultValue) {
        this.columnName = columnName;
        this.destColumnType = destColumnType;
        this.columnDefaultValue = columnDefaultValue;
    }

    public IbfColumnInfo(IbfColumnInfo columnInfo) {
        this.columnName = columnInfo.columnName;
        this.destColumnType = columnInfo.destColumnType;
        this.columnDefaultValue = columnInfo.columnDefaultValue;
    }

    // need these getter methods for accessing these properties in Apache Velocity
    public String getColumnName() {
        return columnName;
    }

    public DataType getDestColumnType() {
        return destColumnType;
    }

    public Object getColumnDefaultValue() {
        return columnDefaultValue;
    }

    @Override
    public String toString() {
        return "{columnName="
                + columnName
                + ", destColumnType="
                + destColumnType
                + ", columnDefaultValue="
                + columnDefaultValue
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        return (this == o)
                || (o != null
                && this.getClass() == o.getClass()
                && this.columnName.equals(((IbfColumnInfo) o).columnName)
                && this.destColumnType.equals(((IbfColumnInfo) o).destColumnType)
                && ((this.columnDefaultValue == null && ((IbfColumnInfo) o).columnDefaultValue == null)
                || (this.columnDefaultValue != null
                && this.columnDefaultValue.equals(
                ((IbfColumnInfo) o).columnDefaultValue))));
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + destColumnType.hashCode();
        if (columnDefaultValue != null) result += columnDefaultValue.hashCode();
        return result;
    }
}
