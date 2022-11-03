package com.example.db;

import com.example.logger.ExampleLogger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;

public class DbInfoLoggingUtils {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public static Optional<String> versionFromMetadata(Supplier<Connection> connection) {
        try (Connection conn = connection.get()) {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            if (dbMetaData.getDatabaseProductName().equals("Oracle")) {
                String singleLineVersion = dbMetaData.getDatabaseProductVersion().replaceAll("\n", "");
                if (singleLineVersion.matches(".* \\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+ .*")) {
                    return Optional.of(
                            singleLineVersion.replaceAll("^.* (\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+) .*$", "$1"));
                }
            }
            return Optional.of(dbMetaData.getDatabaseMajorVersion() + "." + dbMetaData.getDatabaseMinorVersion());
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "Unable to get DB version from metadata", e);
        }
        return Optional.empty();
    }

    public static void logDbVersion(Supplier<Connection> connection, com.example.db.DbServiceType serviceType) {
        Optional<String> version = versionFromMetadata(connection);
        version.ifPresent(v -> DbInfoLoggingUtils.logDbVersion(serviceType, v));
    }

    public static void logDbVersion(com.example.db.DbServiceType serviceType, String version) {
        LOG.info(
                String.format(
                        "Detected database information:id=%s,full_name=%s,hosting_provider=%s,version=%s",
                        serviceType.id(), serviceType.fullName(), serviceType.hostingProvider().name(), version));
    }
}
