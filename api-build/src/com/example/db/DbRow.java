package com.example.db;

import com.example.core.TableRef;
import io.micrometer.core.lang.NonNull;

import java.util.*;
import java.util.stream.Stream;

public class DbRow<T extends com.example.db.DbRowValue> implements Iterable<T> {

    protected final List<T> rowValues = new ArrayList<>();

    public DbRow(T... rowValue) {
        this(Arrays.asList(rowValue));
    }

    public DbRow( Collection<T> c) {
        rowValues.addAll(c);
        assertUniqueness();
    }

    public boolean addAll(T... rowValue) {
        boolean added = rowValues.addAll(Arrays.asList(rowValue));
        assertUniqueness();
        return added;
    }

    public boolean add(T rowValue) {
        boolean added = rowValues.add(rowValue);
        assertUniqueness();
        return added;
    }

    public int size() {
        return rowValues.size();
    }

    @Override
    public Iterator<T> iterator() {
        return rowValues.iterator();
    }

    public Stream<T> stream() {
        return rowValues.stream();
    }

    private void assertUniqueness() {
        Set<String> existingColumnNames = new HashSet<>();
        rowValues.forEach(
                rv -> {
                    if (existingColumnNames.contains(rv.columnName)) {
                        throw new RuntimeException("Duplicate column name " + rv.columnName + " detected in row");
                    }
                    existingColumnNames.add(rv.columnName);
                });
    }

    public List<Object> get(TableRef tableRef) {
        return null;
    }

    public boolean isEmpty() {
        return false;
    }
}
