package com.example.db;

public class DbConnectionException extends Exception {

    public final DbConnectionStatus connectionStatus;

    public DbConnectionException(DbConnectionStatus connectionStatus, Throwable cause) {
        super(cause);
        this.connectionStatus = connectionStatus;
    }

    @Override
    public String getMessage() {
        if (connectionStatus == DbConnectionStatus.OtherError) {
            return getCause().getMessage();
        }
        return connectionStatus.longMessage;
    }
}
