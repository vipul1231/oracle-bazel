package com.example.core;

import com.example.logger.ExampleLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;

import java.time.Instant;
import java.util.*;

public class SchemaConfig {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private Optional<Boolean> excludedByUser = Optional.empty();
    private Optional<String> excludedBySystem = Optional.empty();
    private Optional<String> notRecommended = Optional.empty();

    private Optional<Instant> deletedFromSource = Optional.empty();

    private SortedMap<String, TableConfig> tables = new TreeMap<>();

    private Optional<Instant> firstObservedAt = Optional.empty();

    private Optional<Instant> lastCustomerApprovedAt = Optional.empty();

    // Justification: parameter name is OK.
    @SuppressWarnings("MethodParameterNamingConvention")
    @JsonIgnore
    public void updateFromSource(SchemaConfig fromSource, boolean excludeByDefault, boolean excludeColumnsByDefault) {
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
        this.excludedBySystem = fromSource.excludedBySystem;

        if (this.deletedFromSource.isPresent()) this.deletedFromSource = Optional.empty();

        fromSource.tables.forEach(
                (tableName, sourceTable) -> {
                    TableConfig table =
                            this.tables.computeIfAbsent(
                                    tableName,
                                    newTableName -> {
                                        TableConfig newTable = new TableConfig();

                                        // if the system is excluding this schema for some reason, we do not want to set
                                        // it to user excluded. The user didn't make a choice.
                                        if (!sourceTable.getExcludedBySystem().isPresent()) {
                                            newTable.setExcludedByUser(
                                                    Optional.of(
                                                            sourceTable.getNotRecommended().isPresent()
                                                                    || excludeByDefault));
                                        }
                                        newTable.setNotRecommended(sourceTable.getNotRecommended());
                                        newTable.setSyncMode(sourceTable.getSyncMode());

                                        return newTable;
                                    });

                    table.updateFromSource(sourceTable, excludeByDefault, excludeColumnsByDefault);
                });

        Set<String> deleted = new HashSet<>();

        for (String table : this.tables.keySet()) {
            if (!fromSource.tables.containsKey(table)) deleted.add(table);
        }

        if (!deleted.isEmpty()) {
            LOG.warning("Tables " + Joiner.on(", ").join(deleted) + " are no longer present in source");

            for (String table : deleted) {
                TableConfig tableConfig = this.tables.get(table);
//                Instant deletedFromSource = tableConfig.getDeletedFromSource().orElseGet(ExampleClock.Instant::now);

//                if (deletedFromSource.isBefore(ExampleClock.Instant.now().minus(StandardConfig.DELETE_AFTER)))
//                    this.tables.remove(table);
//                else tableConfig.setDeletedFromSource(Optional.of(deletedFromSource));
            }
        }
    }

//    @JsonIgnore
//    public void updateFromUser(SchemaConfigPatch fromUser) {
//        fromUser.excludedByUser.ifPresent(
//                excludeByDefault -> {
//                    this.excludedByUser = Optional.of(excludeByDefault);
//                });
//
//        fromUser.tables.ifPresent(
//                tables -> {
//                    tables.forEach(
//                            (table, from) -> {
//                                TableConfig into = this.tables.get(table);
//
//                                if (into != null) into.updateFromUser(from);
//                            });
//                });
//    }

    @Override
    public String toString() {
        return "SchemaConfig{"
                + "excludedByUser="
                + excludedByUser
                + ", excludedBySystem="
                + excludedBySystem
                + ", notRecommended="
                + notRecommended
                + ", deletedFromSource="
                + deletedFromSource
                + ", tables="
                + tables
                + ", firstObservedAt="
                + firstObservedAt
                + ", lastCustomerApprovedAt="
                + lastCustomerApprovedAt
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaConfig that = (SchemaConfig) o;
        return Objects.equals(excludedByUser, that.excludedByUser)
                && Objects.equals(excludedBySystem, that.excludedBySystem)
                && Objects.equals(notRecommended, that.notRecommended)
                && Objects.equals(deletedFromSource, that.deletedFromSource)
                && Objects.equals(tables, that.tables)
                && Objects.equals(firstObservedAt, that.firstObservedAt)
                && Objects.equals(lastCustomerApprovedAt, that.lastCustomerApprovedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                excludedByUser,
                excludedBySystem,
                notRecommended,
                deletedFromSource,
                tables,
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

    /** When this entity was deleted from the source */
    public Optional<Instant> getDeletedFromSource() {
        return deletedFromSource;
    }

    public void setDeletedFromSource(Optional<Instant> deletedFromSource) {
        this.deletedFromSource = deletedFromSource;
    }

    public SortedMap<String, TableConfig> getTables() {
        return tables;
    }

    public TableConfig getTable(String key) {
        return tables.get(key);
    }

    public void putTable(String key, TableConfig table) {
        tables.put(key, table);
    }

    public void setTables(SortedMap<String, TableConfig> tables) {
        this.tables = tables;
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

    /** @return whether schema can be used by connector, i.e. it's not excluded or deleted */
    @JsonIgnore
    public boolean isActive() {
        return !getExcludedByUser().orElse(false)
                && !getExcludedBySystem().isPresent()
                && !getDeletedFromSource().isPresent();
    }
}
