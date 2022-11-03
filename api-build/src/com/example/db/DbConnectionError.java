package com.example.db;

public class DbConnectionError {

    public static final String UNKNOWN_HOSTNAME = "The hostname could not be resolved to an IP address";
    public static final String HOST_OR_PORT_INACCESSIBLE =
            "Either the host IP address cannot be reached, the port is not open or connection timed out (you could try again)";
    public static final String USERNAME_OR_PASSWORD_INVALID =
            "The username and password combination was not accepted by the database server";
    public static final String OTHER = "Error while testing database connection";
    public static final String INTERNAL_ERROR = "Internal error while testing database connection";
}
