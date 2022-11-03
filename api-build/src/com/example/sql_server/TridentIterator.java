package com.example.sql_server;

import com.example.core.TableRef;

import java.util.Map;

public class TridentIterator<T> implements AutoCloseable{
    T object;

    @Override
    public void close() throws Exception {

    }

    public boolean hasNext() {
        return false;
    }

    public Map.Entry<TableRef, T> next() {
        return null;
    }

    public Map<TableRef, TridentCursor> getCursors(Map<TableRef, TridentCursor> currentCursors) {
        return null;
    }
}
