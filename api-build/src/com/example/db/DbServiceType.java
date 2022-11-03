package com.example.db;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public interface DbServiceType {
    String id();

    String fullName();

    String description();

    com.example.db.DbHostingProvider hostingProvider();

    String warehouseId();

    /** Specifies keywords to search from UI. For instance, 'Android' and 'Play Store' for Google Play. */
    default Set<String> searchKeywords() {
        return ImmutableSet.of();
    }

    default Set<String> recommendations() {
        return ImmutableSet.of();
    }
}
