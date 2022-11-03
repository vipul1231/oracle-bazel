package com.example.core;

import com.example.core.annotations.DataType;
import com.example.db.DbRowValue;

import java.util.OptionalInt;
import java.util.*;

// TODO split this into SourceColumn and DestinationColumn to differentiate namespaces
public class Column implements Comparable<Column> {

    /** Indicates that column should be skipped */
    public static final Column SKIP = Column.String("\uD83D\uDCA9").build();

    public static final int DEFAULT_PRECISION = 18;
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_LENGTH = 8;
    // TODO: MAX_PRECISION and MAX_SCALE are warehouse specific, they shouldn't be defined here
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SCALE = 37;

    public final String name;
    public final DataType type;
    public final boolean unique;
    public final boolean primaryKey;
    public final boolean deleteFrom;
    public final Optional<String> foreignKey;
    public final OptionalInt byteLength;
    public final OptionalInt precision;
    public final OptionalInt scale;
    public final boolean sortKey;
    public final boolean distKey;
    public final boolean notNull;

    private static final Set<String> WARNED_PRECISION = new HashSet<>();
    private static final Set<String> WARNED_SCALE = new HashSet<>();

    public Column(String name, DataType type, boolean primaryKey ) {
        this.name = name;
        this.type = type;
        unique=false;
        this.primaryKey = primaryKey;
        deleteFrom=false;
        foreignKey=null;
        byteLength=OptionalInt.empty();
        precision=OptionalInt.empty();
        scale=OptionalInt.empty();
        sortKey=false;
        distKey=false;
        notNull=false;
    }


    @Deprecated
    public Column(
            String name,
            DataType type,
            PrimaryKeyType primaryKey,
            Optional<String> foreignKey,
            OptionalInt byteLength,
            OptionalInt precision,
            OptionalInt scale,
            boolean sortKey,
            boolean distKey,
            boolean notNull) {
        this(
                name,
                type,
                primaryKey != PrimaryKeyType.None,
                primaryKey != PrimaryKeyType.None,
                primaryKey == PrimaryKeyType.Strong,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    /** You should probably use the builder patterns like Column.String(...) instead of this constructor */
    public Column(
            String name,
            DataType type,
            boolean unique,
            boolean primaryKey,
            boolean deleteFrom,
            Optional<String> foreignKey,
            OptionalInt byteLength,
            OptionalInt precision,
            OptionalInt scale,
            boolean sortKey,
            boolean distKey,
            boolean notNull) {
        if (byteLength.isPresent() && type != DataType.String && type != DataType.Json && type != DataType.Binary)
            byteLength = OptionalInt.empty();

        if (precision.isPresent() && type != DataType.BigDecimal) precision = OptionalInt.empty();

        if (scale.isPresent() && type != DataType.BigDecimal) scale = OptionalInt.empty();

        if (precision.isPresent()) {
            // If precision < 1, something is very wrong
            if (precision.getAsInt() < 1) throw new IllegalArgumentException("Decimal precision cannot be < 1");
        }

        if (scale.isPresent()) {
            // If scale < 0, something is very wrong
            if (scale.getAsInt() < 0) throw new IllegalArgumentException("Decimal scale cannot be < 0");
        }

        if (primaryKey) {
            if (!notNull)
                throw new IllegalArgumentException(
                        "PRIMARY KEY columns must also be NOT NULL - column " + name + " violates this constraint");

            // Primary key is meaningless if we don't enforce uniqueness
            if (!unique)
                throw new IllegalArgumentException(
                        "PRIMARY KEY columns must also be UNIQUE - column " + name + " violates this constraint");
        }

        // Doing DELETE FROM with null values would require a very expensive query:
        //
        // DELETE FROM table
        // USING staging
        // WHERE table.id = staging.id OR table.id IS NULL AND staging.id IS NULL
        //
        // We do not currently support this operation
        if (deleteFrom && !notNull) {
            throw new IllegalArgumentException(
                    "DELETE FROM columns must also be NOT NULL - column " + name + " violates this constraint");
        }

        Objects.requireNonNull(name, "name is null");
        Objects.requireNonNull(type, "type is null");
        Objects.requireNonNull(foreignKey, "foreignKey is null");
        Objects.requireNonNull(byteLength, "byteLength is null");
        Objects.requireNonNull(precision, "precision is null");
        Objects.requireNonNull(scale, "scale is null");

        this.name = name;
        this.type = type;
        this.unique = unique;
        this.primaryKey = primaryKey;
        this.deleteFrom = deleteFrom;
        this.foreignKey = foreignKey;
        this.byteLength = byteLength;
        this.precision = precision;
        this.scale = scale;
        this.sortKey = sortKey;
        this.distKey = distKey;
        this.notNull = notNull;
    }

    @Deprecated
    public static Column create(
            String name,
            DataType type,
            PrimaryKeyType primaryKey,
            Optional<String> foreignKey,
            OptionalInt byteLength,
            OptionalInt precision,
            OptionalInt scale,
            boolean sortKey,
            boolean distKey,
            boolean notNull) {
        return new Column(name, type, primaryKey, foreignKey, byteLength, precision, scale, sortKey, distKey, notNull);
    }

    public static Column create(
            String name,
            DataType type,
            boolean unique,
            boolean primaryKey,
            boolean deleteFrom,
            Optional<String> foreignKey,
            OptionalInt byteLength,
            OptionalInt precision,
            OptionalInt scale,
            boolean sortKey,
            boolean distKey,
            boolean notNull) {
        return new Column(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return sortKey == column.sortKey
                && distKey == column.distKey
                && notNull == column.notNull
                && Objects.equals(name, column.name)
                && type == column.type
                && unique == column.unique
                && primaryKey == column.primaryKey
                && deleteFrom == column.deleteFrom
                && Objects.equals(foreignKey, column.foreignKey)
                && Objects.equals(byteLength, column.byteLength)
                && Objects.equals(precision, column.precision)
                && Objects.equals(scale, column.scale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    @Override
    public String toString() {
        String s = name + " " + type;

        if (byteLength.isPresent()) s += "(" + byteLength.getAsInt() + ")";
        else if (precision.isPresent() && scale.isPresent())
            s += "(" + precision.getAsInt() + ", " + scale.getAsInt() + ")";
        else if (precision.isPresent()) s += "(" + precision.getAsInt() + ")";

        if (unique) s += " unique";

        if (primaryKey) s += " primary key";

        if (deleteFrom) s += " delete from";

        if (sortKey) s += " sortkey";

        if (distKey) s += " distkey";

        if (foreignKey!=null && foreignKey.isPresent()) s += " references " + foreignKey.get();

        if (notNull) s += " not null";

        return s;
    }

    @Override
    public int compareTo(Column that) {
        int comparePKey = Boolean.compare(this.primaryKey, that.primaryKey);

        if (comparePKey != 0) return comparePKey;

        int compareDeleteFrom = Boolean.compare(this.deleteFrom, that.deleteFrom);

        if (compareDeleteFrom != 0) return compareDeleteFrom;

        return this.name.compareTo(that.name);
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public ColumnType asColumnType() {
        return new ColumnType(type, byteLength, precision, scale, notNull);
    }

    public Column withPrimaryKey(boolean primaryKey, boolean unique, boolean deleteFrom) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull || primaryKey);
    }

    public Column withName(String name) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withType(DataType type) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withLength(OptionalInt byteLength) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withScale(OptionalInt scale) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withScale(int scale) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                OptionalInt.of(scale),
                sortKey,
                distKey,
                notNull);
    }

    public Column withPrecision(OptionalInt precision) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withPrecision(int precision) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                OptionalInt.of(precision),
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withByteLength(int byteLength) {
        return withByteLength(OptionalInt.of(byteLength));
    }

    public Column withByteLength(OptionalInt byteLength) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withColumnType(ColumnType t) {
        return Column.create(
                name,
                t.type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                t.byteLength,
                t.precision,
                t.scale,
                sortKey,
                distKey,
                t.notNull);
    }



    public Column withSortKey(boolean sortKey) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withDistKey(boolean distKey) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }

    public Column withForeignKey(Optional<String> foreignKey) {
        return Column.create(
                name,
                type,
                unique,
                primaryKey,
                deleteFrom,
                foreignKey,
                byteLength,
                precision,
                scale,
                sortKey,
                distKey,
                notNull);
    }



    public static class Builder {
        private final String name;
        private final DataType type;
        private boolean unique = false, primaryKey = false, deleteFrom = false;
        private Optional<String> foreignKey = Optional.empty();
        private OptionalInt byteLength = OptionalInt.empty();
        private OptionalInt precision = OptionalInt.empty();
        private OptionalInt scale = OptionalInt.empty();
        private boolean sortKey = false, distKey = false, notNull = false;

        public Builder(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

        @Deprecated
        public Builder primaryKey(PrimaryKeyType type) {
            unique = type != PrimaryKeyType.None;
            primaryKey = type != PrimaryKeyType.None;
            deleteFrom = type == PrimaryKeyType.Strong;
            notNull = notNull || type != PrimaryKeyType.None;

            return this;
        }

        public Builder unique(boolean key) {
            unique = key;

            return this;
        }

        public Builder primaryKey(boolean key) {
            primaryKey = key;
            deleteFrom = key;
            unique = unique || key;
            notNull = notNull || key;

            return this;
        }

        // todo: unique, deleteFrom and primaryKey are order dependent, this can be confusing
        // todo: if I want a primary key column that is not used as a delete key, I need to do
        // .primaryKey(true).deleteKey(false)
        // todo: if I do it in the opposite order, then deleteKey(false) will be overridden
        public Builder deleteFrom(boolean delete) {
            deleteFrom = delete;
            notNull = notNull || delete;

            return this;
        }

        public Builder sortKey(boolean key) {
            sortKey = key;

            return this;
        }

        public Builder distKey(boolean key) {
            distKey = key;

            return this;
        }

        public Builder foreignKey(String table) {
            foreignKey = Optional.of(table);

            return this;
        }

        public Builder foreignKey(Optional<String> table) {
            foreignKey = table;

            return this;
        }

        public Builder notNull(boolean notNull) {
            if (!notNull && this.primaryKey)
                throw new IllegalArgumentException("Can't set PRIMARY KEY column to NULLABLE");

            this.notNull = notNull;

            return this;
        }

        public Builder byteLength(int byteLength) {
            this.byteLength = OptionalInt.of(byteLength);

            return this;
        }

        public Builder byteLength(OptionalInt byteLength) {
            this.byteLength = byteLength;

            return this;
        }

        public Builder precision(int precision) {
            this.precision = OptionalInt.of(precision);

            return this;
        }

        public Builder precision(OptionalInt precision) {
            this.precision = precision;

            return this;
        }

        public Builder scale(int scale) {
            this.scale = OptionalInt.of(scale);

            return this;
        }

        public Builder scale(OptionalInt scale) {
            this.scale = scale;

            return this;
        }

        public Column build() {
            return Column.create(
                    name,
                    type,
                    unique,
                    primaryKey,
                    deleteFrom,
                    foreignKey,
                    byteLength,
                    precision,
                    scale,
                    sortKey,
                    distKey,
                    notNull || primaryKey);
        }
    }

    public static Builder Int(String name) {
        return new Builder(name, DataType.Int);
    }

    public static Builder Boolean(String name) {
        return new Builder(name, DataType.Boolean);
    }

    public static Builder String(String name) {
        return new Builder(name, DataType.String);
    }

    public static Builder String(String name, int length) {
        return new Builder(name, DataType.String).byteLength(length);
    }

    public static Builder Short(String name) {
        return new Builder(name, DataType.Short);
    }

    public static Builder Long(String name) {
        return new Builder(name, DataType.Long);
    }

    public static Builder Double(String name) {
        return new Builder(name, DataType.Double);
    }

    public static Builder Float(String name) {
        return new Builder(name, DataType.Float);
    }

    /**
     * A BigDecimal without precision or scale can occur when reading JSON. It will be written to the data warehouse as
     * a double.
     */
    public static Builder BigDecimal(String name) {
        return new Builder(name, DataType.BigDecimal);
    }

    public static Builder BigDecimal(String name, int precision, int scale) {
        return new Builder(name, DataType.BigDecimal).precision(precision).scale(scale);
    }

    public static Builder Instant(String name) {
        return new Builder(name, DataType.Instant);
    }

    /** @see Column#Int */
    @Deprecated
    public static Builder Integer(String name) {
        return new Builder(name, DataType.Int);
    }

    public static Builder LocalDate(String name) {
        return new Builder(name, DataType.LocalDate);
    }

    public static Builder LocalDateTime(String name) {
        return new Builder(name, DataType.LocalDateTime);
    }

    public static Builder Json(String name) {
        return new Builder(name, DataType.Json);
    }

    public static Builder Binary(String name) {
        return new Builder(name, DataType.Binary);
    }

    public static Builder Unknown(String name) {
        return new Builder(name, DataType.Unknown);
    }

    public static Builder ColumnType(String name, ColumnType type) {
        return new Builder(name, type.type)
                .byteLength(type.byteLength)
                .precision(type.precision)
                .scale(type.scale)
                .notNull(type.notNull);
    }

    public boolean isSkipped() {
        return this.name.equals(Column.SKIP.name);
    }

    public enum PrimaryKeyType {
        /** Equivalent to primaryKey = true, unique = true, deleteFrom = true */
        Strong,

        /** Equivalent to primaryKey = true, unique = true, deleteFrom = false */
        Weak,

        /** Equivalent to primaryKey = false, unique = false, deleteFrom = false */
        None
    }

}
