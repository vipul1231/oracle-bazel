package com.example.core2;

/**
 * OperationTag provides addition information for an operation. Below are supported tags, Please add newer tags as and
 * when required.
 */
public enum OperationTag {
    // Postgre2020 tags
    IMPORT,
    IMPORT_BY_KEY,
    XMIN,
    WAL,
    IBF,

    // Core tags

    /**
     * Used to identify if connector upserts/updates partial rows because all the fields could not be fetched together
     */
    PARTIAL_ROW,
    /** Used to identify initial sync where connector pull historical data */
    HISTORICAL_SYNC,
    /** Used to identify incremental updates of the data */
    INCREMENTAL_SYNC
}

