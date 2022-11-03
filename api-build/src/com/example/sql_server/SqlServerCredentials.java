package com.example.sql_server;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.example.core.DbCredentials;
import java.util.Optional;

@SuppressWarnings("java:S1104")
public class SqlServerCredentials extends DbCredentials {

    public SqlServerCredentials() {
        super();
    }

    public SqlServerCredentials(DbCredentials creds) {
        super(
                creds.host,
                creds.port,
                creds.database,
                creds.user,
                creds.password,
                creds.tunnelHost,
                creds.tunnelPort,
                creds.tunnelUser,
                creds.alwaysEncrypted);
    }

    @JsonCreator
    public SqlServerCredentials(
            @JsonProperty("host") String host,
            @JsonProperty("port") Integer port,
            @JsonProperty("database") Optional<String> database,
            @JsonProperty("user") String user,
            @JsonProperty("password") String password,
            @JsonProperty("tunnelHost") @JsonAlias("tunnel_host") Optional<String> tunnelHost,
            @JsonProperty("tunnelPort") @JsonAlias("tunnel_port") Optional<Integer> tunnelPort,
            @JsonProperty("tunnelUser") @JsonAlias("tunnel_user") Optional<String> tunnelUser,
            @JsonProperty("alwaysEncrypted") Boolean alwaysEncrypted) {
        super(host, port, database, user, password, tunnelHost, tunnelPort, tunnelUser, alwaysEncrypted);
    }
}
