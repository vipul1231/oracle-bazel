package com.example.snowflakecritic;

import com.example.core.ConnectionType;
import com.example.core.DbCredentials;

import java.util.Optional;

@SuppressWarnings("java:S1104")
public class SnowflakeSourceCredentials extends DbCredentials {
    public enum UpdateMethod {
        TIME_TRAVEL,
        IBF
    }

    public Optional<String> region;
    public Optional<String> storageGcpServiceAccount;

    /**
     * Private key data in the format similar to PEM. Used with {@link
     * //     * com.example.core.SnowflakeCredentials.SnowflakeAuthType#KEY_PAIR} authentication.
     *
     * <p>Due to implementation of the setup form line breaks in the PEM string may be replaced with spaces and PEM
     * header and footer may be missing. Use {@link
     * //     * com.example.core.SnowflakeCredentials#recoverPemPrivateKey()} to
     * recover the PEM format
     */
    public String privateKey;

    /**
     * The passphrase for {@link SnowflakeCredentials#privateKey}.
     *
     * <p>Must be ignored if {@link SnowflakeCredentials#isPrivateKeyEncrypted} is false.
     */
    public String passphrase;

    public Boolean isPrivateKeyEncrypted = false;
    public SnowflakeCredentials.SnowflakeAuthType auth;
    public Optional<String> role;
    public ConnectionType connectionType;
    public UpdateMethod updateMethod = UpdateMethod.IBF;

    public SnowflakeSourceCredentials() {
        super();

        this.auth = SnowflakeCredentials.SnowflakeAuthType.PASSWORD;
        this.role = Optional.empty();
        this.connectionType = ConnectionType.Directly;
        this.region = Optional.empty();
        this.storageGcpServiceAccount = Optional.empty();
    }

    public static SnowflakeSourceCredentials fromSnowflakeCredentials(SnowflakeCredentials credentials) {
        SnowflakeSourceCredentials snowflakeSourceCredentials = new SnowflakeSourceCredentials();

        snowflakeSourceCredentials.host = credentials.host;
        snowflakeSourceCredentials.port = credentials.port;
        snowflakeSourceCredentials.database = credentials.database;
        snowflakeSourceCredentials.user = credentials.user;
        snowflakeSourceCredentials.password = credentials.password;
        snowflakeSourceCredentials.port = credentials.port;
        snowflakeSourceCredentials.region = credentials.region;
        snowflakeSourceCredentials.storageGcpServiceAccount = credentials.storageGcpServiceAccount;
        snowflakeSourceCredentials.privateKey = credentials.privateKey;
        snowflakeSourceCredentials.passphrase = credentials.passphrase;
        snowflakeSourceCredentials.isPrivateKeyEncrypted = credentials.isPrivateKeyEncrypted;
        snowflakeSourceCredentials.auth = credentials.auth;
        snowflakeSourceCredentials.role = credentials.role;

        return snowflakeSourceCredentials;
    };

    public SnowflakeCredentials toSnowflakeCredentials() {
        SnowflakeCredentials snowflakeCredentials = new SnowflakeCredentials();

        snowflakeCredentials.host = this.host;
        snowflakeCredentials.port = this.port;
        snowflakeCredentials.database = this.database;
        snowflakeCredentials.user = this.user;
        snowflakeCredentials.password = this.password;
        snowflakeCredentials.port = this.port;
        snowflakeCredentials.region = this.region;
        snowflakeCredentials.storageGcpServiceAccount = this.storageGcpServiceAccount;
        snowflakeCredentials.privateKey = this.privateKey;
        snowflakeCredentials.passphrase = this.passphrase;
        snowflakeCredentials.isPrivateKeyEncrypted = this.isPrivateKeyEncrypted;
        snowflakeCredentials.auth = this.auth;
        snowflakeCredentials.role = this.role;

        return snowflakeCredentials;
    };

    @Override
    public String toString() {
        return "host='"
                + host
                + ", port="
                + port
                + ", database='"
                + database
                + ", user='"
                + user
                + ", password='"
                + password;
    }
}
