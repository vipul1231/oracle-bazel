package com.example.db;

public class IrrecoverableSQLException extends RuntimeException {

    public IrrecoverableSQLException() {
        super();
    }

    public IrrecoverableSQLException(String message) {
        super(message);
    }

    public IrrecoverableSQLException(String message, Throwable t) {
        super(message, t);
    }
}
