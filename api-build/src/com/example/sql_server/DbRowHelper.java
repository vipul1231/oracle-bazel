package com.example.sql_server;

import com.example.core.TableRef;
import com.example.db.DbRow;
import com.example.db.DbRowValue;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;

public class DbRowHelper {
    public static final String COMPOUND_PK_A = PRIMARY_KEY_NAME + "_A";
    public static final String COMPOUND_PK_B = PRIMARY_KEY_NAME + "_B";

    public int maxColumnsPerTable() {
        return 0;
    }

    public DbRow<DbRowValue> simpleRow(int i, String randomString) {
        return new DbRow<>(
                new DbRowValue.Builder("INT", "id").value(1).primaryKey(true).build());
    }

    public DbRowValue simpleStringColumn(String s, String randomString) {
        return null;
    }

    public DbRow<DbRowValue> primaryKeylessRow(int i) {
        return null;
    }

    public DbRowValue foreignKeyRefRowValue(String s, int i, TableRef parentTableA, String primaryKeyName) {
        return null;
    }

    public DbRow<DbRowValue> simpleRowWithTimestamp(int i, String foo) {
        return new DbRow<>(
                new DbRowValue.Builder("TIMESTAMP", "start_time").value("DEFAULT").build());
    }

    public DbRow<DbRowValue> compoundPrimaryKeyRow(int i, int i1) {
        return null;
    }

    public DbRow<DbRowValue> allTypesRow(int i) {
        DbRow<DbRowValue> row =
                new DbRow<>(
                        new DbRowValue.Builder("INT", PRIMARY_KEY_NAME).value(1).primaryKey(true).build(),
//                        new DbRowValue.Builder("VARCHAR", "name").value("test").build(),
                        new DbRowValue.Builder("TIMESTAMP", "start_time").value("DEFAULT").build(),
                        new DbRowValue.Builder("SQL_VARIANT", "colA").inValue("null").build());

        return row;
    }

    public DbRow<DbRowValue> mixedCaseRow(int i) {
        return null;
    }

    public DbRow<DbRowValue> primaryKeylessAllTypesRow(int i) {
        return null;
    }
}
