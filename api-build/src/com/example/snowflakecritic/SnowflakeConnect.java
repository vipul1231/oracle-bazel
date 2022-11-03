package com.example.snowflakecritic;

import com.example.ConnectToDb;
import com.example.fire.migrated.sql_server.ConnectionParameters;
import com.example.ssh.TunnelDataSource;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class SnowflakeConnect extends ConnectToDb<SnowflakeCredentials> {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static final ConnectToDb<SnowflakeCredentials> INSTANCE = new SnowflakeConnect();

    public TunnelDataSource connectViaPortForwarder(SnowflakeCredentials creds) {
        return new TunnelDataSource(
                connectDirectly(creds),
                () -> {
                    // Snowflake never uses SSH tunnels, and it has its own mechanism for going through the
                    // port forwarder
                    // So there is nothing to do to close a snowflake connection
                });
    }

    @Override
    public DataSource connectDirectly(
            String host,
            int port,
            String user,
            String password,
            String database,
            boolean tunnelCredentials,
            boolean isWarehouse,
            Properties properties,
            ConnectionParameters connectionParams,
            boolean dataSourceIgnored,
            boolean isRunningSetupTest,
            String originalHost) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataSource connectDirectly(SnowflakeCredentials creds) {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }

        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return getConnection(creds.user, creds.password);
            }

            @Override
            public Connection getConnection(String user, String password) throws SQLException {
                try {
                    String url = "jdbc:snowflake://" + creds.host + ":" + creds.port;
                    Properties p = new Properties();

                    p.put("ssl", "on");
                    p.put("user", user);
                    if (creds.auth == SnowflakeCredentials.SnowflakeAuthType.PASSWORD) p.put("password", password);
                    else {
                        //p.put("privateKey", getPrivateKey(creds));
                        p.put("privateKey", creds);
                    }
                    creds.role.ifPresent(role -> p.put("role", role));
                    p.put("db", creds.database.get());
                    p.put("schema", "PUBLIC");
                    p.put("application", "Example");

                    // https://docs.snowflake.net/manuals/sql-reference/parameters.html#lock-timeout
                    p.put("LOCK_TIMEOUT", 3600); // 1 hour

                    return DriverManager.getConnection(url, p);
                } catch (SnowflakeSQLException e) {
//                    if (e.getMessage().contains("Incorrect username or password specified")
//                            || e.getMessage()
//                            .contains(
//                                    "JDBC driver encountered communication error. Message: HTTP status=403")) {
//                        throw CompleteWithTask.reconnectWarehouse("snowflake", e);
//                    }
//                    if (e.getMessage().contains("Error code: 390913")) {
//                        throw CompleteWithTask.create(
//                                new SnowflakeBillingNotEnabled(), new Exception("Snowflake billing not enabled!"));
//                    }
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public PrintWriter getLogWriter() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setLogWriter(PrintWriter out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getLoginTimeout() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setLoginTimeout(int seconds) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Logger getParentLogger() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T unwrap(Class<T> iface) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                throw new UnsupportedOperationException();
            }
        };
    }

//    public static PrivateKey getPrivateKey(SnowflakeCredentials creds) {
//        try {
//            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
//            Security.addProvider(new BouncyCastleProvider());
//            // Read an object from the private key file.
//            PEMParser pemParser = new PEMParser(new StringReader(prepareKey(creds)));
//            Object pemObject = pemParser.readObject();
//            pemParser.close();
//            PrivateKeyInfo privateKeyInfo = null;
//            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
//                // Handle the case where the private key is encrypted.
//                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
//                InputDecryptorProvider pkcs8Prov =
//                        new JceOpenSSLPKCS8DecryptorProviderBuilder().build(creds.passphrase.toCharArray());
//                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
//            } else if (pemObject instanceof PEMKeyPair) {
//                privateKeyInfo = (((PEMKeyPair) pemObject).getPrivateKeyInfo());
//            } else if (pemObject instanceof PrivateKeyInfo) {
//                privateKeyInfo = (PrivateKeyInfo) pemObject;
//            } else throw new RuntimeException("Unknown class encountered");
//            return converter.getPrivateKey(privateKeyInfo);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    private static String prepareKey(SnowflakeCredentials creds) {
        if (creds.privateKey.contains("KEY-----")) {
            return creds.privateKey.replaceFirst("KEY-----", "KEY-----\n").replace("-----END", "\n-----END");
        } else if (creds.isPrivateKeyEncrypted) {
            return "-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
                    + creds.privateKey
                    + "\n-----END ENCRYPTED PRIVATE KEY-----";
        } else {
            return "-----BEGIN RSA PRIVATE KEY-----\n" + creds.privateKey + "\n-----END RSA PRIVATE KEY-----";
        }
    }
}
