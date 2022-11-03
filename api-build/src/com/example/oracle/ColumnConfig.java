package com.example.oracle;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 9:34 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class ColumnConfig {
    /** If exclusion by user is not supported, contains a reason why it is not supported */
    private Optional<String> exclusionNotSupported = Optional.empty();

    private Optional<Boolean> excludedByUser = Optional.empty();

    private Optional<Boolean> isPrimaryKey = Optional.empty();
    private Optional<Boolean> isForeignKey = Optional.empty();

    private boolean hashed;
    private Optional<String> excludedBySystem = Optional.empty();

    /** When this column was deleted from the source */
    private Optional<Instant> deletedFromSource = Optional.empty();

    private Optional<String> notRecommended = Optional.empty();

    private Optional<Instant> firstObservedAt = Optional.empty();

    public ColumnConfig() {}

    @Deprecated
    public static ColumnConfig withSupportsExclude(boolean supportsExclude) {
        return createColumnWithExclusionNotSupported(
                supportsExclude ? Optional.empty() : Optional.of("Column does not support exclusion"), false);
    }

    /**
     * Creates a non-primary key column and specifies if an exclusion is supported or not.
     *
     * @param reason When {@link Optional#empty()}, column can be excluded. When non-empty, specifies a reason why it
     *     cannot be excluded
     */
    public static ColumnConfig withExclusionNotSupported(Optional<String> reason) {
        return createColumnWithExclusionNotSupported(reason, false);
    }

    /**
     * Creates a primary key column with a specific reason why it cannot be excluded
     *
     * @param reason Reason why the column cannot be excluded
     */
    public static ColumnConfig primaryKey(String reason) {
        return createColumnWithExclusionNotSupported(Optional.of(reason), true);
    }

    /** Creates a primary key column */
    public static ColumnConfig primaryKey() {
        return createColumnWithExclusionNotSupported(Optional.of("Column does not support exclusion"), true);
    }

    private static ColumnConfig createColumnWithExclusionNotSupported(Optional<String> reason, boolean isPrimaryKey) {
        ColumnConfig columnConfig = new ColumnConfig();
        columnConfig.exclusionNotSupported = reason;
        columnConfig.isPrimaryKey = Optional.of(isPrimaryKey);
        return columnConfig;
    }

    public boolean excluded() {
        return excludedByUser.orElse(false) || excludedBySystem.isPresent();
    }

    @JsonIgnore
    void updateFromSource(ColumnConfig fromSource) {
        boolean noLongerExcludedBySystem =
                this.excludedBySystem.isPresent() && !fromSource.excludedBySystem.isPresent();

        if (noLongerExcludedBySystem && !this.excludedByUser.isPresent()) {
            this.excludedByUser =
                    fromSource.excludedByUser.isPresent()
                            ? fromSource.excludedByUser
                            : Optional.of(fromSource.notRecommended.isPresent());
        } else if (!fromSource.excludedBySystem.isPresent()
                && !this.excludedByUser.isPresent()
                && fromSource.excludedByUser.isPresent()) {
            this.excludedByUser = fromSource.excludedByUser;
        }

        this.exclusionNotSupported = fromSource.exclusionNotSupported;
        this.excludedBySystem = fromSource.excludedBySystem;
        this.notRecommended = fromSource.notRecommended;

        this.isPrimaryKey = fromSource.isPrimaryKey;
        this.isForeignKey = fromSource.isForeignKey;

        if (this.deletedFromSource.isPresent()) {
            this.deletedFromSource = Optional.empty();
        }
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnConfig that = (ColumnConfig) o;
        return hashed == that.hashed
                && Objects.equals(isPrimaryKey, that.isPrimaryKey)
                && Objects.equals(isForeignKey, that.isForeignKey)
                && Objects.equals(exclusionNotSupported, that.exclusionNotSupported)
                && Objects.equals(excludedByUser, that.excludedByUser)
                && Objects.equals(excludedBySystem, that.excludedBySystem)
                && Objects.equals(notRecommended, that.notRecommended)
                && Objects.equals(deletedFromSource, that.deletedFromSource)
                && Objects.equals(firstObservedAt, that.firstObservedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                exclusionNotSupported,
                excludedByUser,
                hashed,
                excludedBySystem,
                notRecommended,
                deletedFromSource,
                isPrimaryKey,
                isForeignKey,
                firstObservedAt);
    }

    @Override
    public String toString() {
        return "ColumnConfig{"
                + "excludedByUser="
                + excludedByUser
                + ", hashed="
                + hashed
                + ", excludedBySystem="
                + excludedBySystem
                + ", notRecommended="
                + notRecommended
                + ", deletedFromSource="
                + deletedFromSource
                + ", isPrimaryKey="
                + isPrimaryKey
                + ", isForeignKey="
                + isForeignKey
                + ", firstObservedAt="
                + firstObservedAt
                + '}';
    }

    public Optional<Boolean> getExcludedByUser() {
        return excludedByUser;
    }

    public void setExcludedByUser(Optional<Boolean> excludedByUser) {
        this.excludedByUser = excludedByUser;
    }

    public Optional<String> getExclusionNotSupported() {
        return exclusionNotSupported;
    }

    public void setExclusionNotSupported(Optional<String> exclusionNotSupported) {
        this.exclusionNotSupported = exclusionNotSupported;
    }

    public boolean isHashed() {
        return hashed;
    }

    public void setHashed(boolean hashed) {
        this.hashed = hashed;
    }

    public Optional<String> getExcludedBySystem() {
        return excludedBySystem;
    }

    public void setExcludedBySystem(Optional<String> excludedBySystem) {
        this.excludedBySystem = excludedBySystem;
    }

    public Optional<Instant> getDeletedFromSource() {
        return deletedFromSource;
    }

    public void setDeletedFromSource(Optional<Instant> deletedFromSource) {
        this.deletedFromSource = deletedFromSource;
    }

    public Optional<String> getNotRecommended() {
        return notRecommended;
    }

    public void setNotRecommended(Optional<String> notRecommended) {
        this.notRecommended = notRecommended;
    }

    public Optional<Instant> getFirstObservedAt() {
        return firstObservedAt;
    }

    public void setFirstObservedAt(Optional<Instant> firstObservedAt) {
        this.firstObservedAt = firstObservedAt;
    }

    public Optional<Boolean> getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(Optional<Boolean> isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public Optional<Boolean> getIsForeignKey() {
        return isForeignKey;
    }

    public void setIsForeignKey(Optional<Boolean> isForeignKey) {
        this.isForeignKey = isForeignKey;
    }
}