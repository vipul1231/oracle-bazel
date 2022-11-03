package com.example.db;

import java.util.Optional;

public class UncaughtExceptionCaptor implements Thread.UncaughtExceptionHandler {

    private Throwable uncaughtException;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        uncaughtException = e;
    }

    public Optional<Throwable> getUncaughtException() {
        return Optional.ofNullable(uncaughtException);
    }
}
