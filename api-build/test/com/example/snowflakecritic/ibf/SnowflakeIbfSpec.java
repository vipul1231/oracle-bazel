package com.example.snowflakecritic.ibf;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.snowflakecritic.SnowflakeInformer;
import com.example.snowflakecritic.SnowflakeSource;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;
import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;
import static com.example.sql_server.DbRowHelper.COMPOUND_PK_A;
import static com.example.sql_server.DbRowHelper.COMPOUND_PK_B;


public abstract class SnowflakeIbfSpec {
    protected static SnowflakeSource snowflakeSource;
    protected static DataSource dataSource;
    protected static SnowflakeInformer informer;

    protected static final String SCHEMA = "SNOWFLAKE_IBF_TEST";
    protected static final TableRef INT_PK_TABLE = new TableRef(SCHEMA, "INT_PK_TEST");
    protected static final TableRef STRING_PK_TABLE = new TableRef(SCHEMA, "STRING_PK_TEST");
    protected static final TableRef COMPOUND_PK_TABLE = new TableRef(SCHEMA, "COMPOUND_PK_TEST");
    protected static final TableRef POSTGRES_COMPARE_DESTINATION_TABLE =
            new TableRef(SCHEMA, "POSTGRES_TEST_COMPARE_TYPES");
    protected static List<Map<String, Object>> INT_PK_TABLE_DATA;
    protected static List<Map<String, Object>> STRING_PK_TABLE_DATA;
    protected static List<Map<String, Object>> COMPOUND_PK_TABLE_DATA;

    @BeforeClass
    public static void beforeClass() throws SQLException {

        dataSource = snowflakeSource.dataSource;
        informer = new SnowflakeInformer(snowflakeSource, new StandardConfig());

        ArrayList<String> queries = new ArrayList<>();

        setupSchemaQueries(SCHEMA, queries);

        // Setup tables for all tests at once because round-trips to Snowflake are slow
        INT_PK_TABLE_DATA = setupIntPkTableQueries(INT_PK_TABLE, queries);
        STRING_PK_TABLE_DATA = setupStringPkTableQueries(STRING_PK_TABLE, queries);
        COMPOUND_PK_TABLE_DATA = setupCompoundPkTableQueries(COMPOUND_PK_TABLE, queries);

        executeQueries(queries);
    }

    @AfterClass
    public static void afterClass() {

    }

    protected static void executeQueries(ArrayList<String> queries) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement s = connection.createStatement()) {
            s.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", queries.size());
            s.execute(queries.stream().collect(Collectors.joining(";")));
        }
    }

    protected static void setupSchemaQueries(String schemaName, ArrayList<String> queries) {
        queries.add(String.format("DROP SCHEMA IF EXISTS %s", schemaName));
        queries.add(String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
    }

    protected static List<Map<String, Object>> setupIntPkTableQueries(TableRef table, ArrayList<String> queries) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(
                new HashMap<String, Object>() {
                    {
                        put(PRIMARY_KEY_NAME, 1);
                        put(UPDATABLE_COLUMN_NAME, "foo");
                    }
                });

        queries.add(
                String.format(
                        "CREATE TABLE %s (%s BIGINT PRIMARY KEY, %s VARCHAR(32))",
                        table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME));
        queries.add(
                String.format(
                        "INSERT INTO %s (%s, %s) VALUES (%d, %s)",
                        table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME, 1, "'foo'"));

        for (int i = 2; i < 12; i++) {
            queries.add(
                    String.format(
                            "INSERT INTO %s (%s, %s) VALUES (%d, %s)",
                            table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME, i, "'bar" + i + "'"));

            int finalI = i;
            rows.add(
                    new HashMap<String, Object>() {
                        {
                            put(PRIMARY_KEY_NAME, finalI);
                            put(UPDATABLE_COLUMN_NAME, "bar" + finalI);
                        }
                    });
        }

        return rows;
    }

    protected static List<Map<String, Object>> setupStringPkTableQueries(TableRef table, ArrayList<String> queries) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(
                new HashMap<String, Object>() {
                    {
                        put(PRIMARY_KEY_NAME, "aa");
                        put(UPDATABLE_COLUMN_NAME, "foo");
                    }
                });

        queries.add(
                String.format(
                        "CREATE TABLE %s (%s VARCHAR(2) PRIMARY KEY, %s VARCHAR(32))",
                        table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME));
        queries.add(
                String.format(
                        "INSERT INTO %s (%s, %s) VALUES (%s, %s)",
                        table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME, "'aa'", "'foo'"));

        for (int i = 66; i < 76; i++) {
            String letter = Character.toString((char) i);
            String key = letter + letter;

            queries.add(
                    String.format(
                            "INSERT INTO %s (%s, %s) VALUES (%s, %s)",
                            table, PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME, "'" + key + "'", "'bar" + i + "'"));

            int finalI = i;
            rows.add(
                    new HashMap<String, Object>() {
                        {
                            put(PRIMARY_KEY_NAME, key);
                            put(UPDATABLE_COLUMN_NAME, "bar" + finalI);
                        }
                    });
        }

        return rows;
    }

    protected static List<Map<String, Object>> setupCompoundPkTableQueries(TableRef table, ArrayList<String> queries) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(
                new HashMap<String, Object>() {
                    {
                        put(COMPOUND_PK_A, 1);
                        put(COMPOUND_PK_B, "aa");
                        put(UPDATABLE_COLUMN_NAME, "foo");
                    }
                });

        queries.add(
                String.format(
                        "CREATE TABLE %s (%s BIGINT, %s VARCHAR(2), %s VARCHAR(32), PRIMARY KEY (%2$s, %3$s))",
                        table, COMPOUND_PK_A, COMPOUND_PK_B, UPDATABLE_COLUMN_NAME));
        queries.add(
                String.format(
                        "INSERT INTO %s (%s, %s, %s) VALUES (%d, %s, %s)",
                        table, COMPOUND_PK_A, COMPOUND_PK_B, UPDATABLE_COLUMN_NAME, 1, "'aa'", "'foo'"));

        for (int i = 66; i < 76; i++) {
            String letter = Character.toString((char) i);
            int pk_a = i;
            String pk_b = letter + letter;

            queries.add(
                    String.format(
                            "INSERT INTO %s (%s, %s, %s) VALUES (%d, %s, %s)",
                            table,
                            COMPOUND_PK_A,
                            COMPOUND_PK_B,
                            UPDATABLE_COLUMN_NAME,
                            i,
                            "'" + pk_b + "'",
                            "'bar" + i + "'"));

            rows.add(
                    new HashMap<String, Object>() {
                        {
                            put(COMPOUND_PK_A, pk_a);
                            put(COMPOUND_PK_B, pk_b);
                            put(UPDATABLE_COLUMN_NAME, "bar" + pk_a);
                        }
                    });
        }

        return rows;
    }
}
