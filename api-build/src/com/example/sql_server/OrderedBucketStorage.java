package com.example.sql_server;

import com.example.core.TableRef;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OrderedBucketStorage<T> {
    ChangeEntry changeEntry;

    public void delete(Map<TableRef, TridentCursor> readCursors) {
    }

    public boolean hasMore() {
        return false;
    }

    public void remove(TableRef table) {

    }

    public void close() {

    }

    public TridentIterator<T> iterator(Set<TableRef> tables, Map<TableRef, TridentCursor> currentCursors) {
        return null;
    }

    public Optional<ChangeEntry> latestItem(TableRef table) {
        return null;
    }

    public void add(TableRef sourceTable, T v1) {

    }

    public boolean isFull() {
        return false;
    }

    public <K, V> T iterator(HashMap<K, V> kvHashMap) {
        return null;
    }
}
