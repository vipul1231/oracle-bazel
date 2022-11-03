package com.example.core;


import com.example.logger.ExampleLogger;
import com.example.oracle.ColumnConfig;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.*;

public class TableConfig {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    /** This is initialized/modified by updateFromUser(), however, updateFromSource() also initializes in few cases */
    private Optional<Boolean> excludedByUser = Optional.empty();

    private Optional<String> excludedBySystem = Optional.empty();
    private Optional<String> notRecommended = Optional.empty();
    private Optional<String> additionalInfo = Optional.empty();
    private boolean supportsExclude = true;
    private boolean supportsHistoryMode = true;
    private boolean supportsColumnConfig;

    /**
     * Some tables are grouped and users can select only the entire group.
     *
     * <p>This setting allows to make every child in the group selectable.
     */
    private boolean supportsSelectableChildren = false;

    /** The name of table which will use for grouping */
    private Optional<String> groupTable = Optional.empty();

    /** When this entity was deleted from the source */
    private Optional<Instant> deletedFromSource = Optional.empty();

    private List<String> destinationTables = new ArrayList<>();

    private Optional<Instant> firstObservedAt = Optional.empty();

    private Optional<Instant> lastCustomerApprovedAt = Optional.empty();

    /** Column configuration. Optional.empty means it wasn't initialized */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Optional<Map<String, ColumnConfig>> columns = Optional.empty();

    public static final SyncMode DEFAULT_SYNC_MODE = SyncMode.Legacy;

    private SyncMode syncMode = DEFAULT_SYNC_MODE;

    public boolean exclude() {
        return excludedByUser.orElse(false) || excludedBySystem.isPresent();
    }

    public static TableConfig withSupportsExclude(boolean supportsExclude) {
        TableConfig tableConfig = new TableConfig();
        tableConfig.supportsExclude = supportsExclude;
        return tableConfig;
    }

    /**
     * Updates this config with data from {@link Service#standardConfig}
     *
     * @param fromSource table from {@link Service#standardConfig}
     * @param excludeByDefault exclude new tables by default
     * @param excludeColumnsByDefault exclude new columns by default
     */
    // Justification: parameter name is OK.
    @SuppressWarnings("MethodParameterNamingConvention")
    void updateFromSource(TableConfig fromSource, boolean excludeByDefault, boolean excludeColumnsByDefault) {
        boolean noLongerExcludedBySystem =
                this.excludedBySystem.isPresent() && !fromSource.excludedBySystem.isPresent();

        if (noLongerExcludedBySystem && !this.excludedByUser.isPresent()) {
            this.excludedByUser =
                    fromSource.excludedByUser.isPresent()
                            ? fromSource.excludedByUser
                            : Optional.of(fromSource.notRecommended.isPresent() || excludeByDefault);
        } else if (!fromSource.excludedBySystem.isPresent()
                && !this.excludedByUser.isPresent()
                && fromSource.excludedByUser.isPresent()) {
            this.excludedByUser = fromSource.excludedByUser;
        }

        this.notRecommended = fromSource.notRecommended;
        this.additionalInfo = fromSource.additionalInfo;
        this.supportsHistoryMode = fromSource.supportsHistoryMode;
        this.excludedBySystem = fromSource.excludedBySystem;
        this.supportsExclude = fromSource.supportsExclude;
        this.supportsSelectableChildren = fromSource.supportsSelectableChildren;

        if (this.deletedFromSource.isPresent()) {
            this.deletedFromSource = Optional.empty();
        }

        supportsColumnConfig = fromSource.supportsColumnConfig;

        destinationTables = fromSource.destinationTables;
        groupTable = fromSource.groupTable;

        updateColumnsFromSource(fromSource, excludeColumnsByDefault);
        deleteColumnsMissingInSource(fromSource);
    }

    // Justification: parameter name is OK.
    @SuppressWarnings("MethodParameterNamingConvention")
    private void updateColumnsFromSource(TableConfig fromSource, boolean excludeColumnsByDefault) {
        fromSource.columns.ifPresent(
                newColumns -> {
                    if (!this.columns.isPresent()) {
                        this.columns = Optional.of(new HashMap<>());
                    }

                    Map<String, ColumnConfig> columnsValue = this.columns.get();

                    newColumns.forEach(
                            (name, sourceColumn) -> {
                                ColumnConfig into =
                                        columnsValue.computeIfAbsent(
                                                name,
                                                newColumnName ->
                                                        createNewColumnConfig(sourceColumn, excludeColumnsByDefault));

//                                into.updateFromSource(sourceColumn);
                            });
                });
    }

    // Justification: parameter name is OK.
    @SuppressWarnings("MethodParameterNamingConvention")
    private static ColumnConfig createNewColumnConfig(
            ColumnConfig sourceColumnsConfig, boolean excludeColumnsByDefault) {
        ColumnConfig newColumn = new ColumnConfig();
        /*
         * If feature flag is removed and this functionality is rolled out to all users,
         * this needs to be reimplemented. User is not making any decision here about a new column
         * and we can't actually change `excludedByUser` flag here. Instead we need to change interpretation
         * of `null` value for `excludedByUser`. For 'Allow all'/'Allow columns' it means 'allowed' now
         * and for 'Block all' it should mean 'block'.
         */
//        if (!sourceColumnsConfig.getExclusionNotSupported().isPresent()
//                && FlagName.AllowedList.check()
//                && excludeColumnsByDefault) {
//            newColumn.setExcludedByUser(Optional.of(true));
//        }

        return newColumn;
    }

    private void deleteColumnsMissingInSource(TableConfig fromSource) {
        fromSource.columns.ifPresent(
                newColumns -> {
                    if (!this.columns.isPresent()) {
                        this.columns = Optional.of(new HashMap<>());
                    }

                    Map<String, ColumnConfig> columnsValue = this.columns.get();

                    Set<String> columnsToDelete = new HashSet<>();

                    for (String column : columnsValue.keySet()) {
                        if (newColumns.containsKey(column)) continue;

                        LOG.warning("Column \"" + column + "\" is no longer present in source");

                        ColumnConfig config = columnsValue.get(column);
//                        Instant deletedFromSource = config.getDeletedFromSource().orElseGet(exampleClock.Instant::now);

//                        if (deletedFromSource.isBefore(
//                                ExampleClock.Instant.now().minus(StandardConfig.DELETE_AFTER))) {
//                            columnsToDelete.add(column);
//                        } else {
//                            config.setDeletedFromSource(Optional.of(deletedFromSource));
//                        }
                    }

                    columnsValue.keySet().removeAll(columnsToDelete);
                });
    }

    public static TableConfig withColumnConfig(boolean supportsColumnConfig) {
        TableConfig table = new TableConfig();
        table.supportsColumnConfig = supportsColumnConfig;
        return table;
    }

//    void updateFromUser(TableConfigPatch fromUser) {
//        fromUser.excludedByUser.ifPresent(
//                excludedByUser -> {
//                    // The front-end should never allow a user to modify a read only TableConfig
//                    // so this should _never_ be thrown
//                    if (!this.supportsExclude) {
//                        throw new IllegalStateException("Cannot modify read-only table config");
//                    }
//
//                    this.excludedByUser = Optional.of(excludedByUser);
//                });
//
//        fromUser.syncMode.ifPresent(
//                syncMode -> {
//                    this.excludedByUser.ifPresent(
//                            excludedByUser -> {
//                                if (excludedByUser && syncMode == SyncMode.History) {
//                                    throw new IllegalStateException(
//                                            "Cannot change sync mode to History Mode for excluded table");
//                                }
//                            });
//                    this.syncMode = syncMode;
//                });
//
//        fromUser.columns.ifPresent(
//                columns -> {
//                    if (!this.columns.isPresent()) {
//                        this.columns = Optional.of(new HashMap<>());
//                    }
//                    columns.forEach(
//                            (column, from) -> {
//                                ColumnConfig into =
//                                        this.columns.get().computeIfAbsent(column, name -> new ColumnConfig());
//
//                                if (into != null) into.updateFromUser(from);
//                            });
//                });
//    }

    @Override
    public String toString() {
        return "TableConfig{"
                + "excludedByUser="
                + excludedByUser
                + ", excludedBySystem="
                + excludedBySystem
                + ", notRecommended="
                + notRecommended
                + ", additionalInfo="
                + additionalInfo
                + ", supportsSelectableChildren="
                + supportsSelectableChildren
                + ", supportsExclude="
                + supportsExclude
                + ", deletedFromSource="
                + deletedFromSource
                + ", destinationTables="
                + destinationTables
                + ", firstObservedAt="
                + firstObservedAt
                + ", lastCustomerApprovedAt="
                + lastCustomerApprovedAt
                + ", supportsColumnConfig="
                + supportsColumnConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableConfig that = (TableConfig) o;
        return Objects.equals(excludedByUser, that.excludedByUser)
                && Objects.equals(excludedBySystem, that.excludedBySystem)
                && Objects.equals(supportsExclude, that.supportsExclude)
                && Objects.equals(notRecommended, that.notRecommended)
                && Objects.equals(additionalInfo, that.additionalInfo)
                && Objects.equals(supportsSelectableChildren, that.supportsSelectableChildren)
                && Objects.equals(deletedFromSource, that.deletedFromSource)
                && Objects.equals(destinationTables, that.destinationTables)
                && Objects.equals(firstObservedAt, that.firstObservedAt)
                && Objects.equals(lastCustomerApprovedAt, that.lastCustomerApprovedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                excludedByUser,
                excludedBySystem,
                supportsExclude,
                notRecommended,
                additionalInfo,
                supportsSelectableChildren,
                deletedFromSource,
                destinationTables,
                firstObservedAt,
                lastCustomerApprovedAt);
    }

    public Optional<Boolean> getExcludedByUser() {
        return excludedByUser;
    }

    public void setExcludedByUser(Optional<Boolean> excludedByUser) {
        this.excludedByUser = excludedByUser;
    }

    public Optional<String> getExcludedBySystem() {
        return excludedBySystem;
    }

    public void setExcludedBySystem(Optional<String> excludedBySystem) {
        this.excludedBySystem = excludedBySystem;
    }

    public Optional<String> getNotRecommended() {
        return notRecommended;
    }

    public void setNotRecommended(Optional<String> notRecommended) {
        this.notRecommended = notRecommended;
    }

    public Optional<String> getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(Optional<String> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public boolean isSupportsSelectableChildren() {
        return supportsSelectableChildren;
    }

    public void setSupportsSelectableChildren(boolean supportsSelectableChildren) {
        this.supportsSelectableChildren = supportsSelectableChildren;
    }

    public boolean isSupportsExclude() {
        return supportsExclude;
    }

    public void setSupportsExclude(boolean supportsExclude) {
        this.supportsExclude = supportsExclude;
    }

    public boolean isSupportsColumnConfig() {
        return supportsColumnConfig;
    }

    public void setSupportsColumnConfig(boolean supportsColumnConfig) {
        this.supportsColumnConfig = supportsColumnConfig;
    }

    public void setSupportsHistoryMode(boolean supportsHistoryMode) {
        this.supportsHistoryMode = supportsHistoryMode;
    }

    public boolean getSupportsHistoryMode() {
        return supportsHistoryMode;
    }

    public Optional<Instant> getDeletedFromSource() {
        return deletedFromSource;
    }

    public void setDeletedFromSource(Optional<Instant> deletedFromSource) {
        this.deletedFromSource = deletedFromSource;
    }

    public List<String> getDestinationTables() {
        return destinationTables;
    }

    public void setDestinationTables(List<String> destinationTables) {
        this.destinationTables = destinationTables;
    }

    public Optional<Map<String, ColumnConfig>> getColumns() {
        return columns;
    }

    public void setColumns(Optional<Map<String, ColumnConfig>> columns) {
        this.columns = columns;
    }

    public void setSyncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public Optional<String> getGroupTable() {
        return groupTable;
    }

    public void setGroupTable(Optional<String> groupTable) {
        this.groupTable = groupTable;
    }

    public Optional<Instant> getFirstObservedAt() {
        return firstObservedAt;
    }

    public void setFirstObservedAt(Optional<Instant> firstObservedAt) {
        this.firstObservedAt = firstObservedAt;
    }

    public Optional<Instant> getlastCustomerApprovedAt() {
        return lastCustomerApprovedAt;
    }

    public void setLastCustomerApprovedAt(Optional<Instant> lastCustomerApprovedAt) {
        this.lastCustomerApprovedAt = lastCustomerApprovedAt;
    }
}
