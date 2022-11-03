package com.example.snowflakecritic;

import java.util.function.Supplier;

public class SingletonBeanFactory<T> implements Supplier<T> {
    private T value;
    private Supplier<T> supplier;

    public SingletonBeanFactory(Supplier<T> supplier) {
        value = null;
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (value == null)
            value = supplier.get();
        return value;
    }

    public void set(T source) {
        this.value = source;
    }

    public void override(Supplier<T> t) {
        this.supplier = t;
    }
}
