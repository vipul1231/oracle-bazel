package com.example.snowflakecritic;

import com.example.core.ConnectionType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SnowflakeCredentials {

    public enum UpdateMethod {
        TIME_TRAVEL,
        IBF
    }
    private static final String PASSWORD_PLACEHOLDER = "******";

    /** Matches a line in the valid PEM file */
    private static final Pattern PEM_LINE_PATTERN = Pattern.compile("(-----[A-Z\\s]+-----|[a-zA-Z0-9+/=]+)");

    public String host;
    public Integer port;
    public Optional<String> database;
    public String user;

    public String password;

    public Optional<String> region;
    public Optional<String> storageGcpServiceAccount;

    /**
     * Private key data in the format similar to PEM. Used with {@link SnowflakeAuthType#KEY_PAIR} authentication.
     *
     * <p>Due to implementation of the setup form line breaks in the PEM string may be replaced with spaces and PEM
     * header and footer may be missing. Use {@link SnowflakeCredentials#recoverPemPrivateKey()} to recover the PEM
     * format
     */
    public String privateKey = null;

    /**
     * The passphrase for {@link SnowflakeCredentials#privateKey}.
     *
     * <p>Must be ignored if {@link SnowflakeCredentials#isPrivateKeyEncrypted} is false.
     */
    public String passphrase = null;

    public Boolean isPrivateKeyEncrypted = false;
    public SnowflakeAuthType auth;

    public ConnectionType connectionType = ConnectionType.Directly;

    public Cloud getSnowflakeCloud() {
        return region.isPresent() ? region().getKey() : null;
    }

    public String getSnowflakeRegion() {
        return region.isPresent() ? region().getValue() : null;
    }

    public Optional<String> role;

    public SnowflakeCredentials() {
        this.host = "";
        this.port = 0;
        this.database = Optional.empty();
        this.user = "";
        this.password = "";
        this.auth = SnowflakeAuthType.PASSWORD;
        this.role = Optional.empty();
        this.region = Optional.empty();
        this.storageGcpServiceAccount = Optional.empty();
    }

    public SnowflakeCredentials(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.user = builder.user;
        this.password = builder.password;
        this.privateKey = builder.privateKey;
        this.isPrivateKeyEncrypted = builder.isPrivateKeyEncrypted;
        this.passphrase = builder.passphrase;
        this.auth = builder.auth;
        this.role = Optional.ofNullable(builder.role);
        this.connectionType = builder.connectionType;
        this.region = builder.region;
        this.storageGcpServiceAccount = builder.storageGcpServiceAccount;
    }

    /** Recovers properly formatted PEM key from {@link SnowflakeCredentials#privateKey} */
    public Optional<String> recoverPemPrivateKey() {
        if (StringUtils.isEmpty(privateKey)) {
            return Optional.empty();
        }

        String pemType = Boolean.TRUE.equals(isPrivateKeyEncrypted) ? "ENCRYPTED PRIVATE KEY" : "RSA PRIVATE KEY";
        StringJoiner joiner = new StringJoiner(System.lineSeparator()).add("-----BEGIN " + pemType + "-----");

        Matcher m = PEM_LINE_PATTERN.matcher(privateKey);
        while (m.find()) {
            if (!m.group().startsWith("-")) {
                joiner.add(m.group());
            }
        }

        joiner.add("-----END " + pemType + "-----");

        return Optional.of(joiner.toString());
    }

    public static class Builder {
        private String host;
        private Integer port;
        private Optional<String> database;
        private String user;
        private String password;
        private String privateKey;
        private String passphrase;
        private Boolean isPrivateKeyEncrypted = false;
        private SnowflakeAuthType auth;
        private String role;
        private ConnectionType connectionType = ConnectionType.Directly;
        private Optional<String> region = Optional.empty();
        private Optional<String> storageGcpServiceAccount = Optional.empty();

        public Builder(String host, Integer port, Optional<String> database, String user) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.user = user;
        }

        public Builder password(String password) {
            this.auth = SnowflakeAuthType.PASSWORD;
            this.password = password;
            return this;
        }

        public Builder keyPair(String privateKey, Boolean isPrivateKeyEncrypted, String passphrase) {
            this.privateKey = privateKey;
            this.isPrivateKeyEncrypted = isPrivateKeyEncrypted;
            this.passphrase = passphrase;
            this.auth = SnowflakeAuthType.KEY_PAIR;
            return this;
        }

        public Builder withRole(String role) {
            this.role = role;
            return this;
        }

        public Builder withConnectionType(ConnectionType type) {
            this.connectionType = type;
            return this;
        }

        public Builder withRegion(String region) {
            this.region = Optional.ofNullable(region);
            return this;
        }

        public Builder withStorageGcpServiceAccount(String storageGcpServiceAccount) {
            this.storageGcpServiceAccount = Optional.ofNullable(storageGcpServiceAccount);
            return this;
        }

        public SnowflakeCredentials build() {
            return new SnowflakeCredentials(this);
        }
    }

    public enum SnowflakeAuthType {
        PASSWORD,
        KEY_PAIR
    }

    public enum Cloud {
        AWS,
        GCP,
        AZURE
    }

    public Pair<Cloud, String> region() {
        if (region.isPresent()) {
            String reg = region.get().toLowerCase();
            if (reg.startsWith("aws_")) return Pair.of(Cloud.AWS, reg.replace("aws_", ""));

            if (reg.startsWith("azure_")) return Pair.of(Cloud.AZURE, reg.replace("azure_", ""));

            if (reg.startsWith("gcp_")) return Pair.of(Cloud.GCP, reg.replace("gcp_", ""));
        }
        return Pair.of(Cloud.AWS, "default-region");
    }

    @JsonIgnore
    public final String getStorageIntegrationName() {
        return ("example_storage_integration_" + region().getRight()).toUpperCase();
    }

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

    public Map<String, String> connectionConfig() {
        LinkedHashMap<String, String> config = new LinkedHashMap<>();
        config.put("host", host);
        config.put("port", port.toString());
        config.put("database", database.orElse(null));
        config.put("auth", auth.name());
        config.put("user", user);
        if (this.connectionType != null) {
            config.put("connection_type", connectionType.name());
        }

        if (auth == SnowflakeCredentials.SnowflakeAuthType.PASSWORD) {
            config.put("password", PASSWORD_PLACEHOLDER);
        } else if (auth == SnowflakeCredentials.SnowflakeAuthType.KEY_PAIR) {
            config.put("private_key", PASSWORD_PLACEHOLDER);

            if (isPrivateKeyEncrypted != null && isPrivateKeyEncrypted) {
                config.put("is_private_key_encrypted", isPrivateKeyEncrypted.toString());
                config.put("passphrase", PASSWORD_PLACEHOLDER);
            }
        }

        if (region.isPresent()) {
            config.put("snowflake_cloud", region().getKey().toString());
            config.put("snowflake_region", region().getValue());
        }
        return config;
    }

    @SuppressWarnings("PublicInnerClass")
    public static class SnowflakeConfigModifier {

        public Map<String, Object> modify(
                Map<String, Object> config, SnowflakeCredentials credentials, ConfigFormat configFormat) {
            Map<String, Object> result = new HashMap<>(config);
            if (credentials.auth == SnowflakeCredentials.SnowflakeAuthType.PASSWORD) {
                result.remove("private_key");
                result.remove("is_private_key_encrypted");
                result.remove("passphrase");
            } else if (credentials.auth == SnowflakeCredentials.SnowflakeAuthType.KEY_PAIR) {
                result.remove("password");
                if (credentials.isPrivateKeyEncrypted == null || !credentials.isPrivateKeyEncrypted) {
                    result.remove("passphrase");
                }
            }
            return result;
        }
    }

    public enum ConfigFormat {
        // api-v1
        V1_1(1),
        // api-v1 + accept:version=2
        V1_2(2),
        // api-v2
        V2(3);

        public final int version;

        ConfigFormat(int version) {
            this.version = version;
        }
    }
}