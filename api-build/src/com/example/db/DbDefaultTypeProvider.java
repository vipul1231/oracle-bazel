package com.example.db;

import com.example.core.TableRef;
import com.example.core.warning.Warning;
import com.example.logger.ExampleLogger;
import com.example.logger.event.integration.WarningEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public class DbDefaultTypeProvider {

    public enum ServiceType {
        POSTGRES("example.com/docs/databases/postgresql#typetransformationsandmapping"),
        MYSQL("example.com/docs/databases/mysql#typetransformationsandmapping");

        public final String typeMappingURL;

        ServiceType(String typeMappingURL) {
            this.typeMappingURL = typeMappingURL;
        }
    }

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private Consumer<Warning> warner;
    private String owner;
    private String schemaPrefix;
    private String slackChannel;
    private ServiceType service;

    public DbDefaultTypeProvider(
            Consumer<Warning> warner, String owner, String schemaPrefix, String slackChannel, ServiceType service) {
        this.warner = warner;
        this.owner = owner;
        this.schemaPrefix = schemaPrefix;
        this.slackChannel = slackChannel;
        this.service = service;
    }

    public Instant warnAndGetNullInstant(TableRef tableRef, String columnName) {
        return (Instant) warnAndGetDefault(tableRef, columnName);
    }

    public LocalDate warnAndGetNullLocalDate(TableRef tableRef, String columnName) {
        return (LocalDate) warnAndGetDefault(tableRef, columnName);
    }

    public LocalDateTime warnAndGetNullLocalDateTime(TableRef tableRef, String columnName) {
        return (LocalDateTime) warnAndGetDefault(tableRef, columnName);
    }

    public String warnAndGetNullString(TableRef tableRef, String columnName) {
        return (String) warnAndGetDefault(tableRef, columnName);
    }

    Object warnAndGetDefault(TableRef tableRef, String columnName) {
        warn(tableRef, columnName);
        return null;
    }

    private void warn(TableRef tableRef, String column) {
        String message =
                String.format(
                        "Replacing unparsable value in %s.%s. See dashboard warning for more info.",
                        tableRef.toString(), column);

//        warner.accept(new com.example.db.SubstitutedUnparsableValueWarning(tableRef, column, service.typeMappingURL));
        LOG.customerWarning(WarningEvent.warning("Substituted Unparsable Value(s)", message));
    }
}
