package com.example.oracle;

import com.example.core.ConnectionType;
import com.example.core.DbCredentials;
import com.example.core.SetupTest;
import com.example.flag.FeatureFlag;
import com.example.forms.Form;
import com.example.forms.SchemaFormat;

import java.net.URI;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/7/2021<br/>
 * Time: 12:50 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleSetupForm {
    private static final String DEFAULT_SCHEMA_PREFIX = "oracle";
    private final OracleServiceType serviceType;
    private final String publicKey;
    private final ConnectionParameters connectionParameters;

    static final int ARCHIVE_LOG_RETENTION_HOURS_ERROR = 3;
    static final int ARCHIVE_LOG_RETENTION_HOURS_WARN = 24;
    public static final int ARCHIVE_LOG_RETENTION_HOURS_MAX_CHECK = 72;
    static final int FLASHBACK_MIN_UNDO_RETENTION = 86400; // 24 hours in seconds

    public OracleSetupForm(OracleServiceType serviceType, String publicKey, ConnectionParameters connectionParameters) {
        this.serviceType = serviceType;
        this.publicKey = publicKey;
        this.connectionParameters = connectionParameters;
    }

    public URI instructions() {
        switch (serviceType) {
            case ORACLE:
                return URI.create("/docs/databases/oracle/setup-guide");
            case ORACLE_RAC:
                return URI.create("/docs/databases/oracle/rac-setup-guide");
            case ORACLE_RDS:
                return URI.create("/docs/databases/oracle/rds-setup-guide");
            case ORACLE_EBS:
                return URI.create("/docs/applications/oracle-ebs/setup-guide");
            default:
                throw new RuntimeException("Unknown hosting provider");
        }
    };

    public Form form(DbCredentials creds) {
        creds.publicKey = publicKey;
        Form.Builder form = new Form.Builder(creds);
        form = form.withSchema(SchemaFormat.SCHEMA_PREFIX, DEFAULT_SCHEMA_PREFIX);
//        form.addField(createConnectionFields(creds, "IP (1.2.3.4) or domain (your.server.com)", false, true));
        form.addTextField("database", "SID/Service Name ", "", true);
//        form.addField(createSshTunnelFields(creds));
        return form.build();
    }

//    @Override
    public Form form() {
//        return form(new DbCredentials());
        return null;
    }

//    @Override
    public void onSaving(DbCredentials creds) {
        creds.host = creds.host.endsWith("/") ? creds.host.substring(0, creds.host.length() - 1) : creds.host;
    }

//    @Override
    public List<SetupTest<DbCredentials>> tests() {
        String dbHostConnTest = "/docs/databases/oracle";
        String oracleConnTest = "/docs/databases/oracle";

        switch (serviceType) {
            case ORACLE:
                dbHostConnTest += "/setup-guide#allowaccess";
                oracleConnTest += "/setup-guide#createuser";
                break;
            case ORACLE_EBS:
                dbHostConnTest = "/docs/applications/oracle-ebs/setup-guide#allowaccess";
                oracleConnTest = "/docs/applications/oracle-ebs/setup-guide#createuser";
                break;
            case ORACLE_RAC:
                dbHostConnTest += "/rac-setup-guide#allowaccess";
                oracleConnTest += "/rac-setup-guide#createuser";
                break;
            case ORACLE_RDS:
                dbHostConnTest += "/rds-setup-guide#enableaccess";
                oracleConnTest += "/rds-setup-guide#createexampleuser";
                break;
            default:
                throw new RuntimeException("Unknown hosting provider");
        }

        List<SetupTest<DbCredentials>> setupTests = new ArrayList<>();

//        List<SetupTest<DbCredentials>> genericTests =
//                (List<SetupTest<DbCredentials>>) Arrays.asList(
//                        DbSetupFormUtils.sshTunnelTest(
//                                "https://example.com/docs/databases/connection-options", connectionParameters),
//                        dbHostConnectionTest(dbHostConnTest, false, connectionParameters),
//                        oracleVersionTest(oracleConnTest, connectionParameters),
//                        dbaExtentsAccessTest(connectionParameters));

        List<SetupTest<DbCredentials>> incrementalUpdateTests;

        if (FeatureFlag.check("OracleFlashback")) {
            incrementalUpdateTests = Arrays.asList(flashbackRetentionTest(oracleConnTest, connectionParameters));
        } else {
            incrementalUpdateTests =
                    Arrays.asList(
                            ArchiveLogAccessTest(oracleConnTest, connectionParameters),
                            ArchiveLogRetentionTest(oracleConnTest, connectionParameters),
                            SupplementalLoggingTest(oracleConnTest, connectionParameters));
        }

//        setupTests.addAll(genericTests);
        if (serviceType == OracleServiceType.ORACLE_RAC) {
            setupTests.add((oracleRACFlashbackTest(oracleConnTest, connectionParameters)));
        }
        setupTests.addAll(incrementalUpdateTests);
        if (FeatureFlag.check("OracleCertificateVerify")) {
//            setupTests.add(
//                    2,
//                    dbCertificateValidation(
//                            OracleConnect.getInstance(), false, connectionParameters, "SELECT 1 from dual"));
        }
        return setupTests;
    }

    private Object dbHostConnectionTest(String dbHostConnTest, boolean b, ConnectionParameters connectionParameters) {
        return null;
    }

    <C extends DbCredentials> SetupTest<C> ArchiveLogAccessTest(String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Validating archive log access";
            }

//            @Override
            public String errorLabel() {
                return "Unable to validate archive log access";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
//            public Result test(C credentials) {
            public Object test(C credentials) {

                Optional<String> error = runTest(credentials);

                if (error.isPresent()) {

                    String originalUser = (String) credentials.user;
                    String upperCaseUser = originalUser.toUpperCase();

                    credentials.user = upperCaseUser;
                    Optional<String> errorUppercase = runTest(credentials);

                    if (!errorUppercase.isPresent()) {
//                        return Result.failed(
//                                "The user '"
//                                        + originalUser
//                                        + "' does not exist, did you mean '"
//                                        + upperCaseUser
//                                        + "'?");
                    }
                }

                return null;
//                return error.map(Result::failed).orElseGet(Result::passed);
            }

            private Optional<String> runTest(DbCredentials credentials) {
                if (!credentials.database.isPresent()) {
                    return Optional.of("Missing database name");
                }

//                try (OracleSetupTester tester = new OracleSetupTester(getOracleApi(credentials, params))) {
//                    return tester.test();
//                }
                return  null;
            }
        };
    }

    protected OracleApi getOracleApi(DbCredentials credentials, ConnectionParameters params) {
        return new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);
    }

    static <C extends DbCredentials> SetupTest<C> ArchiveLogRetentionTest(
            String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Validating archive log retention period";
            }

//            @Override
            public String errorLabel() {
                return "Unable to validate archive log retention period";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
            public Result test(C credentials) {
                OracleApi api = new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);
                try {
                    int maxLogminerDuration = api.getLogMinerSetupValidator().getMaxLogminerDuration();

                    if (maxLogminerDuration < ARCHIVE_LOG_RETENTION_HOURS_ERROR) {
                        return Result.failed(
                                "Archive log retention period must be more than "
                                        + ARCHIVE_LOG_RETENTION_HOURS_ERROR
                                        + " hours.");
                    } else if (maxLogminerDuration < ARCHIVE_LOG_RETENTION_HOURS_WARN) {
                        return Result.warning(
                                "Archive log retention period is "
                                        + maxLogminerDuration
                                        + " hours. However that is less than our minimum recommendation of "
                                        + ARCHIVE_LOG_RETENTION_HOURS_WARN
                                        + " hours.");
                    } else {
                        return Result.passed(
                                "Archive log retention period is at least " + maxLogminerDuration + " hours.");
                    }
                } catch (Exception e) {
                    return Result.failed(e.getMessage());
                }
            }
        };
    }

    static <C extends DbCredentials> SetupTest<C> dbaExtentsAccessTest(ConnectionParameters params) {
        return new SetupTest<C>() {

//            @Override
            public String label() {
                return "Validating system view permission";
            }

//            @Override
            public String errorLabel() {
                return "Unable to validate system view permission";
            }

//            @Override
            public URI instructions() {
                return URI.create("/docs/databases/oracle/setup-guide#createuser");
            }

//            @Override
            public Result test(C credentials) {
                OracleApi api = new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);

                HashMap<String, Boolean> accessMap = new HashMap<>();
                accessMap.put("DBA_EXTENTS", api.getLogMinerSetupValidator().hasTableAccess("DBA_EXTENTS"));
                accessMap.put("DBA_TABLESPACES", api.getLogMinerSetupValidator().hasTableAccess("DBA_TABLESPACES"));
                accessMap.put("DBA_SEGMENTS", api.getLogMinerSetupValidator().hasTableAccess("DBA_SEGMENTS"));
                Set<String> viewsWithoutAccess =
                        accessMap
                                .entrySet()
                                .stream()
                                .filter(e -> !e.getValue())
                                .map(e -> e.getKey())
                                .collect(Collectors.toSet());

                if (viewsWithoutAccess.isEmpty()) {
                    return Result.passed();
                } else {
                    String views = String.join(", ", viewsWithoutAccess);
                    String commands = new String();

                    for (String view : viewsWithoutAccess) {
                        commands += "\n GRANT SELECT ON " + view + " TO " + credentials.user + ";";
                    }

                    return Result.warning(
                            "Connecting user "
                                    + credentials.user
                                    + " does not have access to the following view(s): "
                                    + views
                                    + ". Enable access to greatly reduce initial import speeds.\n"
                                    + "To grant access, run:"
                                    + commands);
                }
            }
        };
    }

    static <C extends DbCredentials> SetupTest<C> SupplementalLoggingTest(
            String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Access to database-level supplemental logging";
            }

//            @Override
            public String errorLabel() {
                return "Unable to access database-level supplemental logging";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
            public Result test(C credentials) {
                OracleApi api = new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);

                boolean isDbSupplementalLoggingEnabled;
                try {
                    isDbSupplementalLoggingEnabled = api.getLogMinerSetupValidator().isSupplementalLoggingEnabled(api.connectToSourceDb());
                } catch (Exception e) {
                    return Result.warning(e.getMessage());
                }

                if (isDbSupplementalLoggingEnabled) {
                    return Result.passed();
                } else {
                    return Result.warning("Database Level Supplemental logging of primary key columns is not enabled");
                }
            }
        };
    }

    public static <C extends DbCredentials> SetupTest<C> oracleVersionTest(
            String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Validating database version";
            }

//            @Override
            public String errorLabel() {
                return "Unable to validate database version";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
            public Result test(C credentials) {
                TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                OracleApi api = new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);

                String oracleDbVersion = OracleConnect.getInstance().getOracleDbVersion(api.connectToSourceDb());
                boolean isUsingSshTunnel = credentials.getConnectionType() == ConnectionType.SshTunnel;
                boolean isOldOracleVersion = OracleConnect.isOracleVersionOld(oracleDbVersion);

                if (!isUsingSshTunnel && isOldOracleVersion) {
                    return Result.failed(
                            "Oracle database versions 12.1 and below must be connected with an SSH tunnel. "
                                    + "Please refer to the Oracle setup guide for more information.");
                } else {
                    return Result.passed();
                }
            }
        };
    }

    static <C extends DbCredentials> SetupTest<C> flashbackRetentionTest(
            String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Checking Flashback retention";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
            public Result test(C credentials) {
                OracleApi api = new OracleService(OracleServiceType.ORACLE, false).productionApi(credentials, params);
                Connection connection = api.connectToSourceDb();
                int undoLogRetention = api.getUndoLogRetention(connection);
                boolean flashbackDataArchiveEnabled = api.getFlashbackSetupValidator().flashbackDataArchiveEnabled(connection);
                if (undoLogRetention >= FLASHBACK_MIN_UNDO_RETENTION) {
                    return Result.passed("UNDO_RETENTION parameter has been set to at least 24 hours.");
                } else if (flashbackDataArchiveEnabled) {
                    return Result.passed("At least one Flashback Data Archive has been configured.");
                } else {
                    return Result.failed(
                            "UNDO_RETENTION parameter is set to less than 24 hours and there are no Flashback Data "
                                    + "Archives set up. One of these must be true in order to proceed.");
                }
            }
        };
    }

    static <C extends DbCredentials> SetupTest<C> oracleRACFlashbackTest(
            String instructionsUrl, ConnectionParameters params) {
        return new SetupTest<C>() {
//            @Override
            public String label() {
                return "Checking if Flashback is enabled";
            }

//            @Override
            public String errorLabel() {
                return "Unable to use Oracle RAC without Flashback";
            }

//            @Override
            public URI instructions() {
                return URI.create(instructionsUrl);
            }

//            @Override
            public Result test(C credentials) {
                OracleApi api =
                        new OracleService(OracleServiceType.ORACLE_RAC, false).productionApi(credentials, params);
                Connection connection = api.connectToSourceDb();
                boolean flashbackDataArchiveEnabled = api.getFlashbackSetupValidator().flashbackDataArchiveEnabled(connection);
                if (flashbackDataArchiveEnabled) {
                    return Result.passed();
                } else {
                    return Result.failed("Enable Flashback to use Oracle RAC");
                }
            }
        };
    }
}