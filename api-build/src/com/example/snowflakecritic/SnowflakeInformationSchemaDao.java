package com.example.snowflakecritic;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.example.snowflakecritic.SnowflakeSQLUtils.escape;

public class SnowflakeInformationSchemaDao {
    private static String TABLES_QUERY =
            "SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES FROM %s.INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_TYPE = 'BASE TABLE' "
                    + "AND TABLE_SCHEMA != 'INFORMATION_SCHEMA'";

    private static final String BASE_COLUMNS_QUERY =
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
                    + "IS_NULLABLE, DATA_TYPE, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH "
                    + "FROM %s.INFORMATION_SCHEMA.COLUMNS";

    private static String COLUMNS_QUERY = BASE_COLUMNS_QUERY + " WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'";
    private static String TABLE_TRACKING_ENABLED_QUERY = "SHOW TABLES LIMIT 100";

    private static final String TABLE_COLUMNS_QUERY =
            BASE_COLUMNS_QUERY + " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";

    // Snowflake INFORMATION_SCHEMA tables do not contain the column name for primary keys (see TABLE_CONSTRAINTS), so
    // we must use "SHOW PRIMARY KEYS" instead.
    private static String PRIMARY_KEYS_QUERY = "SHOW PRIMARY KEYS IN DATABASE %s";
    private static final String TABLE_PRIMARY_KEYS_QUERY = "SHOW PRIMARY KEYS IN %s.%s.%s";

    private final SnowflakeSource source;
    private final StandardConfig standardConfig;
    private final Map<String, Map<String, TableRef>> tableRefsMap = new HashMap();
    private final Map<TableRef, List<SnowflakeColumnInfo>> columnsMap = new HashMap();
    private final Map<TableRef, List<String>> primaryKeysMap = new HashMap();
    private final Map<TableRef, Set<String>> excludedColumns;

    public SnowflakeInformationSchemaDao(
            SnowflakeSource source, StandardConfig standardConfig, Map<TableRef, Set<String>> excludedColumns) {
        this.source = source;
        this.standardConfig = standardConfig;
        this.excludedColumns = excludedColumns;
    }

    public Collection<SnowflakeColumnInfo> getTableColumnInfo(TableRef tableRef) {
        fetchPrimaryKeyInfo(primaryKeyInfoQuery(tableRef));
        fetchColumnInfo(columnInfoQuery(tableRef));

        return newSnowflakeTableInfo(tableRef, 0, 0).sourceColumnInfo();
    }

    private TableRef fromResultSet(ResultSet resultSet) throws SQLException {
        return getTableRef(resultSet.getString("TABLE_SCHEMA"), resultSet.getString("TABLE_NAME"));
    }

    public Map<TableRef, SnowflakeTableInfo> fetchTableInfo() {
        fetchPrimaryKeyInfo(primaryKeyInfoQuery());
        fetchColumnInfo(columnInfoQuery());

        Map<TableRef, SnowflakeTableInfo> tableInfoMap = new HashMap<>();
        source.executeWithRetry(
                connection -> {
                    try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(tableInfoQuery())) {
                        while (resultSet.next()) {
                            TableRef table = fromResultSet(resultSet);
                            tableInfoMap.put(table, buildTableInfo(table, resultSet));
                        }
                    }
                    try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(TABLE_TRACKING_ENABLED_QUERY)) {
                        while (resultSet.next()) {
                            boolean isChangeTrackingEnabled = "ON".equals(resultSet.getString("change_tracking"));
                            String tableName = resultSet.getString("name");
                            String schemaName = resultSet.getString("schema_name");
                            //System.out.println("Change tracking enabled for " + tableName + " is " + isChangeTrackingEnabled);
                            SnowflakeTableInfo tableInfo = tableInfoMap.get(new TableRef(schemaName, tableName));
                            Preconditions.checkArgument(tableInfo != null);
                            tableInfo.setChangeTrackingEnabled(isChangeTrackingEnabled);
                        }
                    }
                });

        return tableInfoMap;
    }

    private void fetchColumnInfo(String s) {
        source.executeWithRetry(
                connection -> {
                    queryColumns(connection, s);
                });
    }

    private void fetchPrimaryKeyInfo(String query) {
        source.executeWithRetry(
                connection -> {
                    queryPrimaryKeys(connection, query);
                });
    }

    private void queryColumns(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            columnsMap.clear();
            while (resultSet.next()) {
                TableRef table = fromResultSet(resultSet);
                SnowflakeColumnInfo columnInfo = buildColumnInfo(table, resultSet);
                columnsMap.computeIfAbsent(table, __ -> new ArrayList());
                columnsMap.get(table).add(columnInfo);
            }
        }
    }

    private TableRef getTableRef(String schemaName, String tableName) {
        tableRefsMap.computeIfAbsent(schemaName, __ -> new HashMap<>());
        tableRefsMap.get(schemaName).computeIfAbsent(tableName, __ -> new TableRef(schemaName, tableName));

        return tableRefsMap.get(schemaName).get(tableName);
    }

    private String primaryKeyInfoQuery() {
        return String.format(PRIMARY_KEYS_QUERY, escape(source.getDatabase()));
    }

    private String primaryKeyInfoQuery(TableRef tableRef) {
        return String.format(
                TABLE_PRIMARY_KEYS_QUERY, escape(source.getDatabase()), escape(tableRef.schema), escape(tableRef.name));
    }

    private String columnInfoQuery() {
        return String.format(COLUMNS_QUERY, escape(source.getDatabase()));
    }

    private String columnInfoQuery(TableRef tableRef) {
        return String.format(TABLE_COLUMNS_QUERY, escape(source.getDatabase()), tableRef.schema, tableRef.name);
    }

    private String tableInfoQuery() {
        return String.format(TABLES_QUERY, escape(source.getDatabase()));
    }

    private SnowflakeColumnInfo buildColumnInfo(TableRef table, ResultSet resultSet) throws SQLException {
        String name = resultSet.getString("COLUMN_NAME");
        SnowflakeType snowflakeType = SnowflakeType.fromDataTypeString(resultSet.getString("DATA_TYPE"));
        int oridinalPosition = resultSet.getInt("ORDINAL_POSITION");
        OptionalInt byteLength = getOptionalInt(resultSet, "CHARACTER_OCTET_LENGTH");
        OptionalInt precision = getOptionalInt(resultSet, "NUMERIC_PRECISION");
        OptionalInt scale = getOptionalInt(resultSet, "NUMERIC_SCALE");
        OptionalInt maxLength = getOptionalInt(resultSet, "CHARACTER_MAXIMUM_LENGTH");
        boolean isNullable = resultSet.getString("IS_NULLABLE") == "YES" ? true : false;

        boolean isPrimaryKey = false;
        if (primaryKeysMap.containsKey(table)) isPrimaryKey = primaryKeysMap.get(table).contains(name);

        return new SnowflakeColumnInfo(
                table,
                name,
                oridinalPosition,
                snowflakeType,
                snowflakeType.destinationType(),
                byteLength,
                maxLength,
                precision,
                scale,
                !isNullable,
                isPrimaryKey,
                false,
                Optional.empty(),
                excludedColumns.getOrDefault(table, new HashSet<>()).contains(name)
                , false);
    }

    private SnowflakeTableInfo buildTableInfo(TableRef table, ResultSet resultSet) throws SQLException {
        long estimatedRowCount = resultSet.getLong("ROW_COUNT");
        long estimatedDataBytes = resultSet.getLong("BYTES");

        return newSnowflakeTableInfo(table, estimatedRowCount, estimatedDataBytes);
    }

    private SnowflakeTableInfo newSnowflakeTableInfo(TableRef table, long estimatedRowCount, long estimatedDataBytes) {
        return new SnowflakeTableInfo(
                table,
                null,
                estimatedRowCount,
                estimatedDataBytes,
                columnsMap.get(table),
                columnsMap.get(table).size(),
                standardConfig.syncModes().get(table));
    }

    private OptionalInt getOptionalInt(ResultSet resultSet, String columnLabel) throws SQLException {
        OptionalInt value = OptionalInt.of(resultSet.getInt(columnLabel));
        if (resultSet.wasNull()) value = OptionalInt.empty();

        return value;
    }

    private void queryPrimaryKeys(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {


            while (resultSet.next()) {
                TableRef table = getTableRef(resultSet.getString("schema_name"), resultSet.getString("table_name"));
                primaryKeysMap.computeIfAbsent(table, __ -> new ArrayList());
                primaryKeysMap.get(table).add(resultSet.getString("column_name"));
            }
        }
    }
}
