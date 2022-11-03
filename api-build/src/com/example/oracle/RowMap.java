package com.example.oracle;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:42 AM<br/>
 * To change this template use File | Settings | File Templates.
 */

import com.example.core.TableRef;

import java.time.Instant;
import java.util.*;

/** A memory-efficient, mutable {@code Map<String, Object>} with a predefined array of keys and values. */
class RowMap extends AbstractMap<String, Object> {
    private final String[] keys;
    private final Object[] values;
    private final boolean[] present;

    RowMap(String[] keys, Object[] values, boolean[] present) {
        for (String key : keys) Objects.requireNonNull(key, "Key is null");

        this.keys = keys;
        this.values = values;
        this.present = present;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new AbstractSet<Entry<String, Object>>() {
            @Override
            public Iterator<Entry<String, Object>> iterator() {
                return new Iterator<Entry<String, Object>>() {
                    int i = 0;

                    private int nextPresent() {
                        for (int check = i; check < keys.length; check++) {
                            if (present[check]) return check;
                        }

                        return -1;
                    }

                    @Override
                    public boolean hasNext() {
                        return nextPresent() != -1;
                    }

                    @Override
                    public Entry<String, Object> next() {
                        int current = nextPresent();
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        Entry<String, Object> result =
                                new Map.Entry<String, Object>() {
                                    @Override
                                    public String getKey() {
                                        return keys[current];
                                    }

                                    @Override
                                    public Object getValue() {
                                        return values[current];
                                    }

                                    @Override
                                    public Object setValue(Object value) {
                                        Object prev = values[current];

                                        values[current] = value;

                                        return prev;
                                    }
                                };

                        i = current + 1;

                        return result;
                    }
                };
            }

            @Override
            public int size() {
                int count = 0;

                for (boolean each : present) {
                    if (each) count++;
                }

                return count;
            }
        };
    }

    @Override
    public Object get(Object key) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) return values[i];
        }

//        throw new RuntimeException("Key " + key + " is not in pre-defined keys " + Joiner.on(", ").join(keys));
        throw new RuntimeException();
    }

    @Override
    public boolean containsKey(Object key) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) return present[i];
        }

//        throw new RuntimeException("Key " + key + " is not in pre-defined keys " + Joiner.on(", ").join(keys));
        throw new RuntimeException();
    }

    public Object put(String key, String value) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) {
                Object prev = values[i];

                values[i] = value;
                present[i] = true;

                return prev;
            }
        }

//        throw new RuntimeException("Key " + key + " is not in pre-defined keys " + Joiner.on(", ").join(keys));
        throw new RuntimeException();
    }

    @Override
    public BlockRange remove(Object key) {
        throw new UnsupportedOperationException("remove");
    }

    public void put(TableRef table, BlockRange blockRange) {

    }

    public void put(TableRef table, Instant syncStart) {

    }
}