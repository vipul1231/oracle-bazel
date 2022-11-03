package com.example.lambda;

import java.util.function.Supplier;

public class Lazy<T> implements Supplier<T> {
    private final UnsafeSupplier<T> delegate;
    private volatile T value;
    private volatile boolean initialized;

    public Lazy(UnsafeSupplier<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T get() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    value = delegate.get();
                    initialized = true;
                }
            }
        }

        return value;
    }

    public boolean isPresent() {
        return value != null;
    }

    public interface UnsafeSupplier<T> extends Supplier<T> {
        T getUnsafe() throws Exception;

        default T get() {
            try {
                return getUnsafe();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void reset() {
        synchronized (this) {
            initialized = false;
            value = null;
        }
    }
}
