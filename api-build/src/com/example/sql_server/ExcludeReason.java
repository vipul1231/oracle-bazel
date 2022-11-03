package com.example.sql_server;

enum ExcludeReason {
    ERROR_MISSING_SELECT("Missing SELECT permission"),
    ERROR_CDC_AND_CT_NOT_ENABLED("Change Tracking or Change Data Capture must be enabled"),

    ERROR_CT_VIEW_CHANGE_TRACKING("(CT) Missing VIEW CHANGE TRACKING permission"),
    ERROR_CT_SELECT_ON_PK("(CT) Missing SELECT permission on a PRIMARY KEY column"),

    ERROR_CDC_TOO_MANY_INSTANCES("(CDC) Multiple capture instances not supported");

    final String message;

    ExcludeReason(String message) {
        this.message = message;
    }
}
