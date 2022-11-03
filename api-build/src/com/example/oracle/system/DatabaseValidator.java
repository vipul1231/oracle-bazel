package com.example.oracle.system;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.example.oracle.Constants.SYSTEM_SCHEMAS;
import static com.example.oracle.OracleErrorCode.ORA_00904;

public class DatabaseValidator {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    private Optional<Set<TableRef>> isTableSupplementalLoggingEnabled = Optional.empty();
    private Set<TableRef> supplementalLoggingTypeAllEnabledTables = new HashSet<>();
    private Optional<Set<TableRef>> primaryKeylessTablesWithRowMovementEnabled = Optional.empty();
    public static final String SUPPLEMENTAL_LOG_GROUP_ALL_TYPE = "ALL COLUMN LOGGING";

    public enum SupplementalLogging {
        MINIMUM,
        PRIMARY_KEY,
        ALL,
        NONE
    }

    public SupplementalLogging supplementalLoggingEnabled(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet result =
                     statement.executeQuery(
                             "SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL, LOG_MODE, SUPPLEMENTAL_LOG_DATA_PK FROM SYS.V_$DATABASE")) {

            if (!result.next()) throw new RuntimeException("IsArchiveLogModeEnabled query result is empty");

            if (!result.getString("LOG_MODE").equals("ARCHIVELOG"))
                throw new RuntimeException("Database is not in ARCHIVELOG mode");

            boolean supplementalPk = result.getString("SUPPLEMENTAL_LOG_DATA_PK").equals("YES");
            boolean supplementalAll = result.getString("SUPPLEMENTAL_LOG_DATA_ALL").equals("YES");
            boolean supplementalMin = result.getString("SUPPLEMENTAL_LOG_DATA_MIN").equals("YES");

            if (!supplementalPk && !supplementalAll && !supplementalMin) {
                return SupplementalLogging.NONE;
            }

            if (supplementalAll) return SupplementalLogging.ALL;
            else if (supplementalPk) return SupplementalLogging.PRIMARY_KEY;
            else return SupplementalLogging.MINIMUM;

        } catch (Exception e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for SYS.V_$DATABASE");
            else throw new RuntimeException("Could not read from SYS.V_$DATABASE: " + e.getMessage());
        }
    }

    public boolean isTableSupplementalLoggingEnabled(Connection connection, TableRef table) {
        if (!isTableSupplementalLoggingEnabled.isPresent()) {
            try (Statement statement = connection.createStatement();
                 ResultSet result =
                         statement.executeQuery(
                                 "SELECT OWNER, TABLE_NAME, LOG_GROUP_TYPE  FROM SYS.ALL_LOG_GROUPS")) {
                Set<TableRef> loggingEnabled = new HashSet<>();
                while (result.next()) {
                    String schema = result.getString("OWNER");
                    String tableName = result.getString("TABLE_NAME");
                    loggingEnabled.add(new TableRef(schema, tableName));
                    if (SUPPLEMENTAL_LOG_GROUP_ALL_TYPE.equalsIgnoreCase(result.getString("LOG_GROUP_TYPE")))
                        supplementalLoggingTypeAllEnabledTables.add(new TableRef(schema, tableName));
                }
                isTableSupplementalLoggingEnabled = Optional.of(Collections.unmodifiableSet(loggingEnabled));
            } catch (SQLException e) {
                if (e.getMessage().contains("table or view does not exist")) {
                    throw new RuntimeException("Missing SELECT permission for SYS.ALL_LOG_GROUPS");
                } else {
                    throw new RuntimeException("Could not read from SYS.ALL_LOG_GROUPS: " + e.getMessage());
                }
            }
        }

        return isTableSupplementalLoggingEnabled.get().contains(table);
    }

    public boolean isTableSupplementalLoggingTypeAllEnabled(Connection connection, TableRef table) {
        if (isTableSupplementalLoggingEnabled(connection, table))
            return supplementalLoggingTypeAllEnabledTables.contains(table);
        else return false;
    }

    public boolean isPrimaryKeylessTableWithRowMovementEnabled(Connection connection, TableRef table) {
        if (!primaryKeylessTablesWithRowMovementEnabled.isPresent()) {
            Set<TableRef> pkLessTablesWithRMEnabled = new HashSet<>();

            String sql =
                    "SELECT t.OWNER as SCHEMA_NAME, t.TABLE_NAME"
                            + " FROM ALL_TABLES t LEFT JOIN ALL_CONSTRAINTS cons"
                            + "          on t.OWNER = cons.OWNER"
                            + "          and t.TABLE_NAME = cons.TABLE_NAME"
                            + "          and cons.CONSTRAINT_TYPE = 'P'"
                            + " where t.ROW_MOVEMENT = 'ENABLED' AND  t.OWNER NOT IN ("
                            + SYSTEM_SCHEMAS
                            + ")"
                            + " AND cons.CONSTRAINT_TYPE is null";
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {

                while (result.next()) {
                    String schema = result.getString("SCHEMA_NAME");
                    String tableName = result.getString("TABLE_NAME");
                    pkLessTablesWithRMEnabled.add(new TableRef(schema, tableName));
                }
                primaryKeylessTablesWithRowMovementEnabled =
                        Optional.of(Collections.unmodifiableSet(pkLessTablesWithRMEnabled));
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Could not get primary keyless tables with row movement enabled: " + e.getMessage());
            }
        }
        return primaryKeylessTablesWithRowMovementEnabled.get().contains(table);
    }

    public boolean isContainerizedDB(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT CDB FROM V$DATABASE")) {
            if (results.next()) {
                return "YES".equalsIgnoreCase(results.getString("CDB"));
            } else {
                throw new RuntimeException("No value set for CDB");
            }
        } catch (SQLException e) {
            if (ORA_00904.is(e)) {
                return false;
            }
            throw new RuntimeException("Could not check if multitenant is enabled: " + e.getMessage(), e);
        }
    }

    public boolean checkPDBAccess(Connection connection, String pdbName) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("ALTER SESSION SET CONTAINER = " + pdbName);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}