package com.example.db;

public enum DbConnectionStatus {
    OtherError("Connection error", "An error has occurred while trying to connect to the database server"),
    HostnameUnresolved("Unknown hostname", "The hostname could not be resolved to an IP address"),
    HostPortInaccessible(
            "Host/port inaccessible",
            "Either the host IP address cannot be reached, the port is not open or connection timed out (you could try again)"),
    UserPasswordInvalid(
            "User/password invalid", "The username and password combination was not accepted by the database server");

    public final String shortMessage;
    public final String longMessage;

    DbConnectionStatus(String shortMessage, String longMessage) {
        this.shortMessage = shortMessage;
        this.longMessage = longMessage;
    }
}
