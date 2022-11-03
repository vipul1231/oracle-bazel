package com.example.db;

import com.example.core.Column;
import com.example.core.SyncMode;
import com.example.core.TableDefinition;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.oracle.Names;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DbTableInfo<ColumnInfo extends com.example.db.DbColumnInfo<?>> {

    public final TableRef sourceTable;
    public final TableRef destinationTableRef;
    public Optional<Long> estimatedRowCount;
    public Optional<Long> estimatedDataBytes;

    private final List<ColumnInfo> sourceColumnInfo;
    private final List<ColumnInfo> primaryKeys;
    private List<Column> sourceColumns;
    private final boolean hasPrimaryKey;
    private final boolean hasForeignKey;
    private final List<Column> exampleColumns;
    private TableDefinition tableDefinition;
    private final int originalColumnCount;
    private final SyncMode syncMode;

    public DbTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            List<ColumnInfo> sourceColumnInfo,
            int originalColumnCount,
            SyncMode syncMode) {

        this(sourceTableRef, schemaPrefix, null, null, sourceColumnInfo, originalColumnCount, syncMode);
    }

    public DbTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            Long estimatedRowCount,
            Long estimatedDataBytes,
            List<ColumnInfo> sourceColumnInfo,
            int originalColumnCount,
            SyncMode syncMode) {

        this.sourceTable = sourceTableRef;
        this.destinationTableRef = initDestinationTableRef(sourceTableRef, schemaPrefix);
        this.estimatedRowCount = Optional.ofNullable(estimatedRowCount);
        this.estimatedDataBytes = Optional.ofNullable(estimatedDataBytes);
        this.tableDefinition = new TableDefinition();

        if (null == sourceColumnInfo) {
            // Create an empty list by default to avoid NullPointerExceptions
            sourceColumnInfo = new ArrayList<>();
        }
        this.sourceColumnInfo = sortSourceColumnInfos(sourceColumnInfo);
        this.sourceColumns = initSourceColumns(this.sourceColumnInfo);
        this.hasPrimaryKey = sourceColumnInfo().stream().anyMatch(ci -> ci.isPrimaryKey);
        this.primaryKeys =
                Collections.unmodifiableList(
                        sourceColumnInfo.stream().filter(ci -> ci.isPrimaryKey).collect(Collectors.toList()));
        this.hasForeignKey = sourceColumnInfo().stream().anyMatch(ci -> ci.isForeignKey);
        this.exampleColumns = initexampleColumns(hasPrimaryKey());
//        this.tableDefinition = initTableDefinition(destinationTableRef, exampleColumns, syncMode);
        this.originalColumnCount = originalColumnCount;
        this.syncMode = syncMode;
    }

    public Optional<Double> estimatedSizeInGigaBytes() {
        return estimatedDataBytes.map(bytes -> bytes / 1_000_000_000D);
    }

    public static TableRef initDestinationTableRef(TableRef sourceTableRef, String schemaPrefix) {
        return new TableRef(
                com.example.db.SchemaNameMapper.defaultMapper(schemaPrefix).toOutputSchema(sourceTableRef.schema),
                sourceTableRef.name);
    }

    /**
     * WARNING: do not use non-static member-variables in concrete implementations of this method. It is invoked within
     * a constructor. We don't initialize example_deleted here because this function doesn't know anything about
     * history mode.
     */
    protected abstract List<Column> initexampleColumns(boolean hasPrimaryKey);

    protected List<Column> initSourceColumns(Collection<ColumnInfo> sourceColumnInfo) {
        return sourceColumnInfo
                .stream()
                .map(ci -> columnInfoToColumn(ci, ci.destinationType))
                .collect(Collectors.toList());
    }

    protected List<Column> initSourceColumns() {
        sourceColumns = initSourceColumns(sourceColumnInfo);
        return sourceColumns;
    }

    /*
    protected TableDefinition initTableDefinition(
            TableRef destinationTableRef, List<Column> exampleColumns, SyncMode syncMode) {

        List<Column> includedColumns =
                includedColumnInfo()
                        .stream()
                        .map(ci -> columnInfoToColumn(ci, ci.destinationType))
                        .collect(Collectors.toList());

        if (syncMode == SyncMode.Legacy) exampleColumns.add(Column.Boolean(Names.example_DELETED_COLUMN).build());

        List<Column> knownTypes =
                Stream.of(includedColumns, exampleColumns).flatMap(Collection::stream).collect(Collectors.toList());

        TableDefinition.Builder builder =
                TableDefinition.prepareTableDefinition(destinationTableRef, knownTypes)
                        .allowNullPrimaryKeys()
                        .withMutableColumnTypes();

        if (syncMode == SyncMode.History) builder = builder.withHistoryModeWrite();

        return builder.build();
    }

     */

    protected void renewTableDefinition() {
        this.sourceColumns = initSourceColumns(sourceColumnInfo);
        tableDefinition = initTableDefinition(destinationTableRef, exampleColumns, syncMode);
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public int totalDestinationColumns() {
        return sourceColumns.size() + exampleColumns.size();
    }

    public int originalColumnCount() {
        return originalColumnCount;
    }

    public boolean hasPrimaryKey() {
        return hasPrimaryKey;
    }

    public boolean hasForeignKey() {
        return hasForeignKey;
    }

    public Collection<ColumnInfo> sourceColumnInfo() {
        return sourceColumnInfo;
    }

    public List<ColumnInfo> primaryKeys() {
        return primaryKeys;
    }

    public List<Column> sourceColumns() {
        return sourceColumns;
    }

    public Set<ColumnInfo> includedColumnInfo() {
        return sourceColumnInfo.stream().filter(ci -> !ci.excluded).collect(Collectors.toCollection( LinkedHashSet::new ));
    }

    public TableDefinition tableDefinition() {
        tableDefinition.setTableRef(sourceTable);
        return tableDefinition;
    }

    /**
     * return new Column(
     * columnInfo.columnName,
     * desiredType,
     * columnInfo.isPrimaryKey,
     * columnInfo.isPrimaryKey,
     * columnInfo.isPrimaryKey,
     * columnInfo.foreignKey,
     * columnInfo.byteLength,
     * columnInfo.numericPrecision,
     * columnInfo.numericScale,
     * false,
     * false,
     * columnInfo.isPrimaryKey);
     *
     * @param columnInfo
     * @param desiredType
     * @return
     */
    private Column columnInfoToColumn(ColumnInfo columnInfo, DataType desiredType) {
        return new Column(columnInfo.columnName, desiredType, columnInfo.isPrimaryKey);
    }

    private List<ColumnInfo> sortSourceColumnInfos(List<ColumnInfo> columnInfosList) {
        return columnInfosList
                .stream()
                .sorted(Comparator.comparingInt(col -> col.ordinalPosition))
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableInfo)) return false;
        DbTableInfo<?> that = (DbTableInfo<?>) o;
        return hasPrimaryKey == that.hasPrimaryKey
                && hasForeignKey == that.hasForeignKey
                && originalColumnCount == that.originalColumnCount
                && sourceColumnInfo.equals(that.sourceColumnInfo)
                && exampleColumns.equals(that.exampleColumns)
                && tableDefinition.equals(that.tableDefinition)
                && syncMode == that.syncMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceColumnInfo,
                hasPrimaryKey,
                hasForeignKey,
                exampleColumns,
                tableDefinition,
                originalColumnCount,
                syncMode);
    }

    @Override
    public String toString() {
        return "DbTableInfo{"
                + "sourceTable="
                + sourceTable
                + ", destinationTableRef="
                + destinationTableRef
                + ", estimatedRowCount="
                + estimatedRowCount
                + ", estimatedDataBytes="
                + estimatedDataBytes
                + ", sourceColumnInfo="
                + sourceColumnInfo
                + ", primaryKeys="
                + primaryKeys
                + ", sourceColumns="
                + sourceColumns
                + ", hasPrimaryKey="
                + hasPrimaryKey
                + ", hasForeignKey="
                + hasForeignKey
                + ", exampleColumns="
                + exampleColumns
                + ", tableDefinition="
                + tableDefinition
                + ", originalColumnCount="
                + originalColumnCount
                + ", syncMode="
                + syncMode
                + '}';
    }

    protected TableDefinition initTableDefinition(
            TableRef destinationTableRef, List<Column> exampleColumns, SyncMode syncMode) {

        List<Column> includedColumns =
                includedColumnInfo()
                        .stream()
                        .map(ci -> columnInfoToColumn(ci, ci.destinationType))
                        .collect(Collectors.toList());

        if (syncMode == SyncMode.Legacy) exampleColumns.add(Column.Boolean(Names.example_DELETED_COLUMN).build());

        List<Column> knownTypes =
                Stream.of(includedColumns, exampleColumns).flatMap(Collection::stream).collect(Collectors.toList());

        TableDefinition.Builder builder =
                TableDefinition.prepareTableDefinition(destinationTableRef, knownTypes)
                        .allowNullPrimaryKeys()
                        .withMutableColumnTypes();

        if (syncMode == SyncMode.History) builder = builder.withHistoryModeWrite();

        return builder.build();
    }
}
