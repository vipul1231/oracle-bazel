package com.example.oracle.flashback;

import com.example.core.TableRef;
import com.example.oracle.SetupValidator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * validation on the Flashback setup
 * <p>
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/9/2021<br/>
 * Time: 3:05 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class FlashbackSetupValidator extends SetupValidator {

    private Optional<Set<TableRef>> isTableFlashbackEnabled = Optional.empty();

    public boolean isTableFlashbackEnabled(Connection connection, TableRef table) {

        if (!isTableFlashbackEnabled.isPresent()) {
            try (Statement statement = connection.createStatement();
                 ResultSet result =
                         statement.executeQuery("SELECT OWNER_NAME, TABLE_NAME FROM dba_flashback_archive_tables")) {
                Set<TableRef> flashbackEnabled = new HashSet<>();
                while (result.next()) {
                    String schema = result.getString("OWNER_NAME");
                    String tableName = result.getString("TABLE_NAME");
                    flashbackEnabled.add(new TableRef(schema, tableName));
                }
                isTableFlashbackEnabled = Optional.of(Collections.unmodifiableSet(flashbackEnabled));
            } catch (SQLException e) {
                if (e.getMessage().contains("table or view does not exist")) {
                    throw new RuntimeException("Missing SELECT permission for dba_flashback_archive_tables", e);
                } else {
                    throw new RuntimeException(
                            "Could not read from dba_flashback_archive_tables: " + e.getMessage(), e);
                }
            }
        }

        return isTableFlashbackEnabled.get().contains(table);
    }

    public boolean flashbackDataArchiveEnabled(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet r = statement.executeQuery("SELECT flashback_archive_name FROM dba_flashback_archive")) {
            return (r.next());
        } catch (SQLException e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for dba_flashback_archive", e);
            throw new RuntimeException("Could not look up flashback archive: " + e.getMessage(), e);
        }
    }
}