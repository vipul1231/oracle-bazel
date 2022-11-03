package com.example.db;

import com.example.core.TableName;
import com.example.core.TableRef;
import com.google.common.base.Throwables;

public class TableError {
//    public final TableName table;
    public final Throwable error;
    public final String code;

    public TableError(TableName table, Throwable error) {
//        this.table = table;
        this.error = error;
        this.code = null;
    }

    public TableError(TableRef table, Throwable error) {
//        this.table = table.toTableName();
        this.error = error;
        this.code = null;
    }

    @Deprecated
    public TableError(TableName table, Throwable error, String code) {
//        this.table = table;
        this.error = error;
        this.code = code;
    }

    @Override
    public String toString() {
//        String message = table.toString();
        Throwable cause = Throwables.getRootCause(error);
//        message += ": " + cause.getClass().getSimpleName();
//        if (cause.getMessage() != null) message += ": " + cause.getMessage();
//        if (code != null) message += ": CODE " + code;
//        return message;
        return null;
    }
}
