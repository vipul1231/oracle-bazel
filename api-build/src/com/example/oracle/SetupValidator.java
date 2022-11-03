package com.example.oracle;

import com.example.core.TableRef;

import java.sql.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.example.oracle.Util.doubleQuote;

/**
 * Common Validations of the setup
 * <p>
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/9/2021<br/>
 * Time: 7:04 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class SetupValidator {

    private Optional<Set<TableRef>> isTableSupplementalLoggingEnabled = Optional.empty();

    public boolean isSupplementalLoggingEnabled(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet result =
                     statement.executeQuery("SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_PK FROM SYS.V_$DATABASE")) {

            if (!result.next()) throw new RuntimeException("IsArchiveLogModeEnabled query result is empty");

            if (!result.getString("LOG_MODE").equals("ARCHIVELOG"))
                throw new RuntimeException("Database is not in ARCHIVELOG mode");

            return result.getString("SUPPLEMENTAL_LOG_DATA_PK").equals("YES");

        } catch (Exception e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for SYS.V_$DATABASE");
            else throw new RuntimeException("Could not read from SYS.V_$DATABASE: " + e.getMessage());
        }
    }

    public boolean isTableSupplementalLoggingEnabled(Connection connection, TableRef table) {
        if (!isTableSupplementalLoggingEnabled.isPresent()) {
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT OWNER, TABLE_NAME FROM SYS.ALL_LOG_GROUPS")) {
                Set<TableRef> loggingEnabled = new HashSet<>();
                while (result.next()) {
                    String schema = result.getString("OWNER");
                    String tableName = result.getString("TABLE_NAME");
                    loggingEnabled.add(new TableRef(schema, tableName));
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

    public boolean hasTableAccess(String tableName) {
        Transactions.RetryFunction<Boolean> action =
                (c) -> {
                    try (PreparedStatement statement =
                                 c.prepareStatement("SELECT * FROM " + doubleQuote(tableName) + " WHERE ROWNUM = 1")) {
                        statement.execute();
                        return true;
                    } catch (SQLException e) {
                        return false;
                    }
                };

        return ConnectionFactory.getInstance().retry("hasTableAccess(" + tableName + ")", action);
    }
}