package com.example.snowflakecritic;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class SnowflakeSourceCredentialsSpec {

    public static String EXAMPLE_HOST = "test.snowflakecomputing.com";
    private static final Integer EXAMPLE_PORT = 443;
    private static final String EXAMPLE_DATABASE = "TESTDB";
    private static final String EXAMPLE_USER = "user";
    private static final String EXAMPLE_PASSWORD = "p@assW0RD!";

    @Test
    public void fromSnowflakeCredentials_convertsCorrectly() {
        SnowflakeCredentials snowflakeCredentials =
                new SnowflakeCredentials.Builder(
                        EXAMPLE_HOST, EXAMPLE_PORT, Optional.of(EXAMPLE_DATABASE), EXAMPLE_USER)
                        .password(EXAMPLE_PASSWORD)
                        .build();

        SnowflakeSourceCredentials snowflakeSourceCredentials =
                SnowflakeSourceCredentials.fromSnowflakeCredentials(snowflakeCredentials);

        assertEquals(EXAMPLE_HOST, snowflakeSourceCredentials.host);
        assertEquals(EXAMPLE_PORT, snowflakeSourceCredentials.port);
        assertEquals(Optional.of(EXAMPLE_DATABASE), snowflakeSourceCredentials.database);
        assertEquals(EXAMPLE_USER, snowflakeSourceCredentials.user);
        assertEquals(EXAMPLE_PASSWORD, snowflakeSourceCredentials.password);
        assertEquals(snowflakeCredentials.region, snowflakeSourceCredentials.region);
        assertEquals(
                snowflakeCredentials.storageGcpServiceAccount, snowflakeSourceCredentials.storageGcpServiceAccount);
        assertEquals(snowflakeCredentials.privateKey, snowflakeSourceCredentials.privateKey);
        assertEquals(snowflakeCredentials.passphrase, snowflakeSourceCredentials.passphrase);
        assertEquals(snowflakeCredentials.isPrivateKeyEncrypted, snowflakeSourceCredentials.isPrivateKeyEncrypted);
        assertEquals(snowflakeCredentials.auth, snowflakeSourceCredentials.auth);
        assertEquals(snowflakeCredentials.role, snowflakeSourceCredentials.role);
//        assertEquals(DbCredentials.ConnectionType.Directly, snowflakeSourceCredentials.connectionType);
    }

    @Test
    public void toSnowflakeCredentials_convertsCorrectly() {
        SnowflakeCredentials snowflakeCredentials =
                new SnowflakeCredentials.Builder(
                        EXAMPLE_HOST, EXAMPLE_PORT, Optional.of(EXAMPLE_DATABASE), EXAMPLE_USER)
                        .password(EXAMPLE_PASSWORD)
                        .build();
        SnowflakeSourceCredentials snowflakeSourceCredentials =
                SnowflakeSourceCredentials.fromSnowflakeCredentials(snowflakeCredentials);
        SnowflakeCredentials recoveredSnowflakeCredentails = snowflakeSourceCredentials.toSnowflakeCredentials();

        assertEquals(EXAMPLE_HOST, recoveredSnowflakeCredentails.host);
        assertEquals(EXAMPLE_PORT, recoveredSnowflakeCredentails.port);
        assertEquals(Optional.of(EXAMPLE_DATABASE), recoveredSnowflakeCredentails.database);
        assertEquals(EXAMPLE_USER, recoveredSnowflakeCredentails.user);
        assertEquals(EXAMPLE_PASSWORD, recoveredSnowflakeCredentails.password);
        assertEquals(snowflakeCredentials.region, recoveredSnowflakeCredentails.region);
        assertEquals(
                snowflakeCredentials.storageGcpServiceAccount, recoveredSnowflakeCredentails.storageGcpServiceAccount);
        assertEquals(snowflakeCredentials.privateKey, recoveredSnowflakeCredentails.privateKey);
        assertEquals(snowflakeCredentials.passphrase, recoveredSnowflakeCredentails.passphrase);
        assertEquals(snowflakeCredentials.isPrivateKeyEncrypted, recoveredSnowflakeCredentails.isPrivateKeyEncrypted);
        assertEquals(snowflakeCredentials.auth, recoveredSnowflakeCredentails.auth);
        assertEquals(snowflakeCredentials.role, recoveredSnowflakeCredentails.role);
//        assertEquals(DbCredentials.ConnectionType.Directly, recoveredSnowflakeCredentails.connectionType);
    }
}
