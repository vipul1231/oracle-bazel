package com.example.sql_server.exceptions;

public class ChangeDataCaptureException extends Exception {
    public ChangeDataCaptureException(String message) {
        super(message);
    }

    public ChangeDataCaptureException(String message, Throwable cause) {
        super(message, cause);
    }
}
