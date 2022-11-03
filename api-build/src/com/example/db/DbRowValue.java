package com.example.db;

import com.example.core.TableRef;
import jdk.nashorn.internal.objects.ArrayBufferView;

import java.util.Optional;

public class DbRowValue {

    public static final String FOREIGN_KEY_NAME = "FOREIGN_KEY";

    public static final String UPDATABLE_COLUMN_NAME = "UPDATABLE_COLUMN";
    public static final String PRIMARY_KEY_NAME = "PRIMARY_KEY";
    public static final String SORT_KEY_NAME = "SORT_KEY";

    public final String columnType;
    public final String columnName;
    public final Object inputValue;
    public final Object outputValue;
    public final Optional<Integer> outputByteLength;
    public final Optional<Integer> outputPrecision;
    public final Optional<Integer> outputScale;
    public final boolean primaryKey;
    public final boolean sortKey;

    public final Optional<String> columnDescriptor;
    public final Optional<TableRef> referencedTableRef;
    public final Optional<String> referencedColumn;

    private DbRowValue(
            String columnType,
            String columnName,
            Object inputValue,
            Object outputValue,
            Integer outputByteLength,
            Integer outputPrecision,
            Integer outputScale,
            boolean primaryKey,
            boolean sortKey,
            String columnDescriptor,
            TableRef referencedTableRef,
            String referencedColumn) {

        this.columnType = columnType;
        this.columnName = columnName;
        this.inputValue = inputValue;
        this.outputValue = outputValue;
        this.outputByteLength = Optional.ofNullable(outputByteLength);
        this.outputPrecision = Optional.ofNullable(outputPrecision);
        this.outputScale = Optional.ofNullable(outputScale);
        this.primaryKey = primaryKey;
        this.sortKey = sortKey;

        this.columnDescriptor = Optional.ofNullable(columnDescriptor);
        this.referencedTableRef = Optional.ofNullable(referencedTableRef);
        this.referencedColumn = Optional.ofNullable(referencedColumn);
    }

    @Override
    public String toString() {
        return "DbRowValue{"
                + "columnType='"
                + columnType
                + '\''
                + ", columnName='"
                + columnName
                + '\''
                + ", inputValue="
                + inputValue
                + ", outputValue="
                + outputValue
                + ", outputByteLength="
                + outputByteLength
                + ", outputPrecision="
                + outputPrecision
                + ", outputScale="
                + outputScale
                + ", primaryKey="
                + primaryKey
                + ", sortKey="
                + sortKey
                + ", columnDescriptor="
                + columnDescriptor
                + ", referencedTableRef="
                + referencedTableRef
                + ", referencedColumn="
                + referencedColumn
                + '}';
    }

    public static class Builder {

        protected final String columnType;
        protected final String columnName;
        protected Object inputValue;
        protected Object outputValue;
        protected Integer outputByteLength;
        protected Integer outputPrecision;
        protected Integer outputScale;
        protected boolean primaryKey;

        // these fields are to protect against improper/duplicate calls
        private boolean ioCalled;
        private boolean inCalled;
        private boolean outCalled;

        private String columnDescriptor;
        private TableRef referencedTableRef;
        private String referencedColumn;

        public Builder(String columnType, String columnName) {
            this.columnType = columnType;
            this.columnName = columnName;
            this.primaryKey = false;
            this.sortKey = false;

            this.ioCalled = this.inCalled = this.outCalled = false;
        }

        public DbRowValue build() {
            validateBuild();
            return new DbRowValue(
                    columnType,
                    columnName,
                    inputValue,
                    outputValue,
                    outputByteLength,
                    outputPrecision,
                    outputScale,
                    primaryKey,
                    sortKey,
                    columnDescriptor,
                    referencedTableRef,
                    referencedColumn);
        }

        public Builder value(Object ioValue) {
            assert !inCalled && !outCalled && !ioCalled;
            this.ioCalled = true;

            this.inputValue = this.outputValue = ioValue;
            return this;
        }

        // TODO: Duplicate of value() method above
        public Builder inValue(Object inputValue) {
            assert !inCalled && !outCalled && !ioCalled;
            this.inCalled = true;

            this.inputValue = inputValue;
            return this;
        }

        public Builder descriptor(String columnDescriptor) {
            this.columnDescriptor = columnDescriptor;
            return this;
        }

        private boolean sortKey;

        public Builder sortKey(boolean sortKey) {
            this.sortKey = sortKey;

            return this;
        }

        public Builder foreignKey(TableRef referencedTableRef, String referencedColumn) {
            this.referencedTableRef = referencedTableRef;
            this.referencedColumn = referencedColumn;
            return this;
        }

        /**
         * As we migrate to the new testing interface, we will rely solely upon {@link
         * com.example.testing.connector_testing.RecordComparator} for asserting output.
         *
         * <p>TODO remove this method once all databases are migrated
         */
        @Deprecated
        public Builder outValue(Object outputValue) {
            assert !outCalled && !ioCalled;
            this.outCalled = true;

            this.outputValue = outputValue;
            return this;
        }

        public Builder outByteLength(int outputByteLength) {
            this.outputByteLength = outputByteLength;
            return this;
        }

        public Builder outPrecision(int outputPrecision) {
            this.outputPrecision = outputPrecision;
            return this;
        }

        public Builder outScale(int outputScale) {
            this.outputScale = outputScale;
            return this;
        }

        public Builder primaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public void validateBuild() {
            assert columnType != null;
            assert columnName != null;
        }

        public ArrayBufferView foreignKey(Optional<String> referencedTable) {
            return null;
        }
    }
}
