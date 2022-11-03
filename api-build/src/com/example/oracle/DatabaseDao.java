package com.example.oracle;

import com.example.core.TableRef;
import com.example.flag.FeatureFlag;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.example.oracle.Constants.DEFAULT_FETCH_SIZE;
import static com.example.oracle.Constants.SYSTEM_SCHEMAS;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/13/2021<br/>
 * Time: 8:57 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class DatabaseDao {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    /**
     * Get current SCN from the database
     *
     * @return Long of SCN
     */
    Long getCurrentScn() {
        ConnectionFactory.getInstance().connectToSourceDb();
        return ConnectionFactory.getInstance().retry("getCurrentScn", connection -> getCurrentScn(connection));
    }

    /**
     * for Flashback: get current SCN *
     */
    Long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
            if (!result.next()) throw new RuntimeException("CURRENT_SCN query reply is empty");
            return result.getLong("CURRENT_SCN");
        }
    }

    /**
     * Names of all tables
     */
    public Map<TableRef, Optional<String>> tables() {
        LOG.info("Getting list of tables from oracle source db");

//        @Language("SQL")
        String selectTables =
                "SELECT DISTINCT OWNER as TABLE_SCHEMA, TABLE_NAME FROM all_tables "
                        + "WHERE OWNER NOT IN ("
                        + SYSTEM_SCHEMAS
                        + ") "
                        + "AND TEMPORARY = 'N' AND (DROPPED = 'NO' OR DROPPED IS NULL) AND "
                        + "(TABLESPACE_NAME NOT IN ('SYSTEM', 'SYSAUX') OR TABLESPACE_NAME IS NULL)";

        if (!FeatureFlag.check("OracleIncludeIotTables")) selectTables += " AND IOT_TYPE IS NULL";

        Map<TableRef, Optional<String>> tables = new HashMap<>();

        String finalSelectTables = selectTables;
        Transactions.RetryFunction<Map<TableRef, Optional<String>>> action =
                (connection) -> {
                    boolean checkTablePermission = FeatureFlag.check("OracleFlashback");
                    Set<TableRef> tablesWithPermission =
                            checkTablePermission ? tablesWithPermission(connection) : new HashSet<>();
                    try (Statement statement = connection.createStatement();
                         ResultSet results = statement.executeQuery(finalSelectTables)) {

                        results.setFetchSize(DEFAULT_FETCH_SIZE);
                        // Fetch all tables in all schemas
                        while (results.next()) {
                            String schema = results.getString("TABLE_SCHEMA");
                            String table = results.getString("TABLE_NAME");

                            Optional<String> excludeBySystem = Optional.empty();
                            if (table.startsWith("MLOG$_"))
                                excludeBySystem = Optional.of("Exclude materialized view log table");
                            else if (checkTablePermission
                                    && !tablesWithPermission.contains(new TableRef(schema, table))) {
                                excludeBySystem =
                                        Optional.of("SELECT permission has not been granted to the example user");
                            }
                            tables.put(new TableRef(schema, table), excludeBySystem);
                        }

                        return tables;
                    }
                };

        return ConnectionFactory.getInstance().retry("tables", t -> new RuntimeException("Error in getting list of all source tables", t), action);
    }

    static Set<TableRef> tablesWithPermission(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result =
                     statement.executeQuery(
                             "SELECT OWNER, OBJECT_NAME AS TABLE_NAME FROM ALL_OBJECTS WHERE OWNER NOT IN ("
                                     + SYSTEM_SCHEMAS
                                     + ")"
                                     + (FlagName.OracleFilterNonTableTypesFromPermissionCheck.check()
                                     ? " AND OBJECT_TYPE = 'TABLE'"
                                     : ""))) {
            Set<TableRef> tablesWithPermission = new HashSet<>();
            result.setFetchSize(DEFAULT_FETCH_SIZE);
            while (result.next()) {
                String schema = result.getString("OWNER");
                String table = result.getString("TABLE_NAME");
                tablesWithPermission.add(new TableRef(schema, table));
            }
            return tablesWithPermission;
        }
    }
}