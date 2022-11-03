package com.example.sql_server;

import com.example.core.TableRef;
import com.example.db.DbRow;
import com.example.db.DbRowValue;

public class SqlServerRowHelper extends DbRowHelper {
    @Override
    public int maxColumnsPerTable() {
        return super.maxColumnsPerTable();
    }

    @Override
    public DbRow<DbRowValue> simpleRow(int i, String randomString) {
        return super.simpleRow(i, randomString);
    }

    @Override
    public DbRowValue simpleStringColumn(String s, String randomString) {
        return super.simpleStringColumn(s, randomString);
    }

    @Override
    public DbRow<DbRowValue> primaryKeylessRow(int i) {
        DbRow<DbRowValue> row = new DbRow<DbRowValue>();
        return new DbRow<>(
                new DbRowValue.Builder("INT", "id").value(1).build());
    }

    @Override
    public DbRowValue foreignKeyRefRowValue(String s, int i, TableRef parentTableA, String primaryKeyName) {
        return super.foreignKeyRefRowValue(s, i, parentTableA, primaryKeyName);
    }

    @Override
    public DbRow<DbRowValue> simpleRowWithTimestamp(int i, String foo) {
        return super.simpleRowWithTimestamp(i, foo);
    }

    @Override
    public DbRow<DbRowValue> compoundPrimaryKeyRow(int i, int i1) {
        return super.compoundPrimaryKeyRow(i, i1);
    }
}
