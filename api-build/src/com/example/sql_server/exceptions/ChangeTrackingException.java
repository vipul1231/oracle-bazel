package com.example.sql_server.exceptions;

public class ChangeTrackingException extends Exception {
    public ChangeTrackingException(String message) {
        super(message);
    }

    public ChangeTrackingException(String message, Throwable cause) {
        super(message, cause);
    }
}
