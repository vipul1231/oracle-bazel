package com.example.sql_server;

import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableName;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.db.DbInfoLoggingUtils;
import com.example.db.DbInformer;
import com.example.db.DbServiceType;
import com.example.db.SqlDbServiceTypeDeterminer;
import com.example.flag.FlagName;
import com.example.logger.ExampleLogger;
import com.example.oracle.ColumnConfigInformation;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.LongTaskTimer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * This class is responsible for inspecting the current state of the source database. The goal here is to make a class
 * that can compile everything the Importer and IncrementalUpdater need to know.
 */
public class SqlServerInformer extends DbInformer<SqlServerSource, SqlServerTableInfo> {
    public static final ExampleLogger LOG = ExampleLogger.getMainLogger();
//    LongTaskTimer metadataTimer =
//            exampleConnectorMetrics.get()
//                    .longTaskTimer(MetricDefinition.SYNC_DURATION, TagName.SYNC_PHASE.toString(), "metadata");
    LongTaskTimer metadataTimer = null;

    public SqlServerInformer(SqlServerSource source, StandardConfig standardConfig) {
        super(source, standardConfig);
    }

    @Override
    public DbServiceType inferServiceType() {
        try (Connection connection = source.connection()) {
            DbServiceType type = SqlServerServiceType.SQL_SERVER;
            try (PreparedStatement ps =
                            connection.prepareStatement(
                                    "SELECT SERVERPROPERTY('ServerName'), SERVERPROPERTY('Edition')");
                    ResultSet results = ps.executeQuery()) {

                if (results.next()) {
                    if (SqlDbServiceTypeDeterminer.checkQueryResult(
                            server -> server.startsWith("EC2AMAZ"), results, 1)) {
                        type = SqlServerServiceType.SQL_SERVER_RDS;
                    } else if (SqlDbServiceTypeDeterminer.checkQueryResult(
                            edition -> edition.toLowerCase().matches("azure"), results, 2)) {
                        type = SqlServerServiceType.SQL_SERVER_AZURE;
                    }
                }
            } catch (SQLException ignore) {
                // nothing to do: this means we cannot infer the service type so just return the default
            }

            return type;
        } catch (SQLException e) {
            throw new RuntimeException("Failure while trying to infer service type", e);
        }
    }

    @Override
    public Optional<String> version() {
        try (Connection connection = source.connection()) {
            return DbInfoLoggingUtils.versionFromMetadata(() -> connection);
        } catch (SQLException e) {
//            LOG.log(Level.WARNING, "Failed to establish connection to source while trying to fetch version", e);
        }
        return Optional.empty();
    }

    @Override
    protected TableRef extractTable(TableName tableName) {
        // TODO check
        return null;
//        return TableRef.fromTableName(tableName);
    }

    @Override
    public Map<TableRef, SqlServerTableInfo> latestTableInfo() {
//        LongTaskTimer.Sample metadataTimerSample = metadataTimer.start();
        Map<TableRef, SqlServerTableInfo> latestTableInfo =
                source.executeWithRetry(
                        connection -> {
                            Map<TableRef, TablePermissions> tableColumnPermissions = getTablePermissionMap(connection);
                            Map<TableRef, List<SqlServerKey>> keysMap = getKeysMap(connection);
                            Map<TableRef, Long> ctTables = getCTTableSet(connection);
                            CDCInfo cdcInfo = getCDCInfo(connection);
                            Map<TableRef, Set<RawColumnInfo>> rawColumnInfoMap = getRawColumnInformationMap(connection);

                            return tableColumnPermissions
                                    .keySet()
                                    .stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Function.identity(),
                                                    tableRef ->
                                                            getTableInfo(
                                                                    tableRef,
                                                                    tableColumnPermissions.get(tableRef),
                                                                    rawColumnInfoMap.getOrDefault(
                                                                            tableRef, new HashSet<>()),
                                                                    keysMap,
                                                                    ctTables,
                                                                    cdcInfo)));
                        });
//        metadataTimerSample.stop();
        return latestTableInfo;
    }

    protected void updateMinChangeVersions() {
        Map<TableRef, Long> updatedMinVersions = source.executeWithRetry(SqlServerInformer::getCTTableSet);
        for (TableRef ctTable : filterForChangeType(allTables(), SqlServerChangeType.CHANGE_TRACKING)) {
            tableInfo(ctTable).minChangeVersion = updatedMinVersions.get(ctTable);
        }
    }

    public List<TableRef> filterForChangeType(Collection<TableRef> includedTables, SqlServerChangeType changeType) {
        return includedTables
                .stream()
                .filter(table -> tableInfo(table).changeType == changeType)
                .collect(Collectors.toList());
    }

    /**
     * The worker bee of this class. Performs the required queries to figure out what each table's capabilities are.
     *
     * @param tableRef The schema and name of the table being refreshed
     */
    private SqlServerTableInfo getTableInfo(
            TableRef tableRef,
            TablePermissions tablePerms,
            Set<RawColumnInfo> rawColumns,
            Map<TableRef, List<SqlServerKey>> keysMap,
            Map<TableRef, Long> ctTables,
            CDCInfo cdcInfo) {

        RawTableInfo rawTableInfo = new RawTableInfo(tableRef);

        if (tablePerms.selectableColumns.isEmpty()) {
            rawTableInfo.changeType = SqlServerChangeType.NONE;
            rawTableInfo.excludeReasons.add(ExcludeReason.ERROR_MISSING_SELECT);
            return rawTableInfo.convertToTableInfo();
        }

        rawTableInfo.canViewChangeTracking = tablePerms.viewChangedTrackingEnabled;

        List<SqlServerKey> keys = keysMap.getOrDefault(tableRef, new ArrayList<>());
        rawTableInfo.keys.addAll(keys);
        rawTableInfo.hasPrimaryKey = keys.stream().anyMatch(SqlServerKey::isPrimaryKey);
        rawTableInfo.hasClusteredIndex = keys.stream().anyMatch(k -> k.clustered);
        rawTableInfo.originalColumnCount = rawColumns.size();
        rawTableInfo.columns =
                getColumnInfoList(
                        rawTableInfo,
                        rawColumns,
                        tablePerms.selectableColumns,
                        keysMap,
                        cdcInfo.columnMap.getOrDefault(tableRef, Collections.emptySet()));
        rawTableInfo.syncMode = syncModes().get(tableRef);

        Set<String> captureInstances = cdcInfo.captureInstanceMap.getOrDefault(tableRef, new HashSet<>());

        if (rawTableInfo.hasPrimaryKey && ctTables.containsKey(tableRef)) {
            rawTableInfo.changeType = SqlServerChangeType.CHANGE_TRACKING;
            rawTableInfo.minChangeVersion = ctTables.get(tableRef);
            configureChangeTracking(rawTableInfo);
        } else if (cdcInfo.isCDCEnabled && !captureInstances.isEmpty()) {
            rawTableInfo.changeType = SqlServerChangeType.CHANGE_DATA_CAPTURE;
            Set<String> cdcColumns = cdcInfo.columnMap.getOrDefault(tableRef, new HashSet<>());
            configureChangeDataCapture(rawTableInfo, cdcColumns, captureInstances);
        } else {
            rawTableInfo.changeType = SqlServerChangeType.NONE;
            rawTableInfo.excludeReasons.add(ExcludeReason.ERROR_CDC_AND_CT_NOT_ENABLED);
        }

        return rawTableInfo.convertToTableInfo();
    }

    private void configureChangeTracking(RawTableInfo rawTableInfo) {
        if (!rawTableInfo.canViewChangeTracking) {
            rawTableInfo.excludeReasons.add(ExcludeReason.ERROR_CT_VIEW_CHANGE_TRACKING);
        }

        Set<String> readableColumns = rawTableInfo.columns.stream().map(col -> col.columnName).collect(toSet());

        Set<String> primaryKeyColumns =
                rawTableInfo
                        .keys
                        .stream()
                        .filter(SqlServerKey::isPrimaryKey)
                        .findFirst()
                        .map(k -> k.columns.keySet())
                        .orElseGet(HashSet::new);

        if (!readableColumns.containsAll(primaryKeyColumns)) {
            rawTableInfo.excludeReasons.add(ExcludeReason.ERROR_CT_SELECT_ON_PK);
        }
    }

    private void configureChangeDataCapture(
            RawTableInfo rawTableInfo, Set<String> cdcColumns, Set<String> captureInstances) {
        if (!cdcColumns.containsAll(rawTableInfo.getColumnNames())) {
            ArrayList<String> diff = new ArrayList<>(Sets.difference(rawTableInfo.getColumnNames(), cdcColumns));
//            warnings.add(new CdcNewColumnWarning(rawTableInfo.sourceTable, diff));
        }

        if (captureInstances.size() == 1) {
            rawTableInfo.cdcCaptureInstance = captureInstances.iterator().next();
        } else {
            rawTableInfo.excludeReasons.add(ExcludeReason.ERROR_CDC_TOO_MANY_INSTANCES);
        }
    }

    private List<SqlServerColumnInfo> getColumnInfoList(
            RawTableInfo rawTable,
            Set<RawColumnInfo> rawColumns,
            Set<String> selectableColumns,
            Map<TableRef, List<SqlServerKey>> keysMap,
            Set<String> cdcColumns) {
        Set<String> excludedColumns = excludedColumns(rawTable.sourceTable);

        return rawColumns
                .stream()
                // filter for select-able columns only
                .filter(rc -> selectableColumns.contains(rc.columnName))
                // filter for supported types only
                .filter(rc -> rc.destinationType.isPresent())
                // Sort columns by ordinal position
                .sorted(Comparator.comparingInt(rc -> rc.ordinalPosition))
                .map(
                        rawColumn -> {
                            Optional<String> foreignKey =
                                    getFKReferenceTableName(rawTable.keys, rawColumn.columnName, keysMap)
                                            .map(ref -> ref.name);

                            Map<String, Integer> primaryKeyOrdinalPositions =
                                    keysMap.getOrDefault(rawTable.sourceTable, Collections.emptyList())
                                            .stream()
                                            .filter(SqlServerKey::isPrimaryKey)
                                            .findFirst()
                                            .map(k -> k.columns)
                                            .orElse(new HashMap<>());

                            return new SqlServerColumnInfo(
                                    rawTable.sourceTable,
                                    rawColumn.columnName,
                                    rawColumn.ordinalPosition,
                                    primaryKeyOrdinalPositions.getOrDefault(rawColumn.columnName, 0),
                                    rawColumn.sourceType,
                                    rawColumn.destinationType.get(),
                                    rawColumn.byteLength,
                                    rawColumn.numericPrecision,
                                    rawColumn.numericScale,
                                    foreignKey.isPresent(),
                                    false,
                                    rawColumn.isNullable.equals("NO"),
                                    isPartOfPrimaryKey(rawTable.keys, rawColumn.columnName),
                                    foreignKey,
                                    excludedColumns.contains(rawColumn.columnName),
                                    cdcColumns.contains(rawColumn.columnName));
                        })
                .collect(toList());
    }

    /**
     * @param key
     * @param columnName
     * @param keysMap
     * @return
     */
    private static boolean acceptSqlServerKey(
            SqlServerKey key, String columnName, Map<TableRef, List<SqlServerKey>> keysMap) {
        return key.type == SqlServerKey.Type.FOREIGN
                && key.columns.containsKey(columnName)
                && referencedTableIsValid(key, keysMap)
                && referencedTableIsExclusive(key.referencedTable.get(), keysMap)
                && referencedColumnsArePrimaryKey(key.referencedTable.get(), key.referenceColumns, keysMap);
    }

    private static Optional<TableRef> getFKReferenceTableName(
            List<SqlServerKey> keys, String columnName, Map<TableRef, List<SqlServerKey>> keysMap) {
        Optional<TableRef> result = Optional.empty();
        if (null != keys) {
            List<Optional<TableRef>> fkTableSet =
                    keys.stream()
                            .filter(k -> acceptSqlServerKey(k, columnName, keysMap))
                            .map(k -> k.referencedTable)
//                            .sorted(Comparator.comparing(Optional::get))
                            .collect(toList());

            if (!fkTableSet.isEmpty()) {
                result = fkTableSet.get(0);
            }
        }

        return result;
    }

    private static boolean referencedTableIsValid(SqlServerKey key, Map<TableRef, List<SqlServerKey>> keysMap) {
        return key.referencedTable.isPresent()
                && keysMap.containsKey(key.referencedTable.get())
                && key.referencedTable.get().schema.equals(key.table.schema);
    }

    private static boolean referencedColumnsArePrimaryKey(
            TableRef refTable, Set<String> refColumns, Map<TableRef, List<SqlServerKey>> keysMap) {
        return keysMap.get(refTable)
                .stream()
                .filter(SqlServerKey::isPrimaryKey)
                .anyMatch(k -> k.columns.keySet().equals(refColumns));
    }

    private static boolean referencedTableIsExclusive(TableRef table, Map<TableRef, List<SqlServerKey>> keysMap) {
        long numReferencesToTable =
                keysMap.values()
                        .stream()
                        .flatMap(List::stream)
                        .filter(k -> k.referencedTable.isPresent() && table.equals(k.referencedTable.get()))
                        .count();

        return numReferencesToTable == 1;
    }

    private static boolean isPartOfPrimaryKey(List<SqlServerKey> keys, String columnName) {
        return keys != null
                && keys.stream().filter(SqlServerKey::isPrimaryKey).anyMatch(k -> k.columns.containsKey(columnName));
    }

    static Map<TableRef, List<SqlServerKey>> getKeysMap(Connection c) throws SQLException {

        String query =
                "SELECT OBJECT_SCHEMA_NAME(t.object_id)            AS TABLE_SCHEMA,"
                        + "       OBJECT_NAME(t.object_id)                   AS TABLE_NAME,"
                        + "       o.name                                     AS KEY_NAME,"
                        + "       CASE o.type"
                        + "         WHEN 'PK' THEN 'PRIMARY'"
                        + "         WHEN 'UQ' THEN 'UNIQUE'"
                        + "         WHEN 'F' THEN 'FOREIGN'"
                        + "         END                                      AS KEY_TYPE,"
                        + "       c.name                                     AS COLUMN_NAME,"
                        + "       i.index_id                                 AS INDEX_ID,"
                        + "       OBJECT_SCHEMA_NAME(f.referenced_object_id) AS REFERENCED_SCHEMA,"
                        + "       OBJECT_NAME(f.referenced_object_id)        AS REFERENCED_TABLE,"
                        + (FlagName.SqlServerOrderByPK.check() ? "ic.key_ordinal AS KEY_ORDINAL," : "")
                        + "       fc.name                                    AS REFERENCED_COLUMN_NAME"
                        + " FROM sys.tables t"
                        + "       LEFT JOIN sys.schemas s"
                        + "                 ON s.schema_id = t.schema_id"
                        + "       LEFT JOIN sys.objects o"
                        + "                 ON o.parent_object_id = t.object_id"
                        + "       LEFT JOIN sys.indexes i"
                        + "                 ON i.name = o.name"
                        + "                   AND i.object_id = t.object_id"
                        + "       LEFT JOIN sys.index_columns ic"
                        + "                 ON ic.object_id = t.object_id"
                        + "                   AND ic.index_id = i.index_id"
                        + "       LEFT JOIN sys.foreign_keys f"
                        + "                 ON f.parent_object_id = t.object_id"
                        + "                   AND f.name = o.name"
                        + "       LEFT JOIN sys.foreign_key_columns fkc"
                        + "                 ON fkc.constraint_object_id = f.object_id"
                        + "       LEFT JOIN sys.columns c"
                        + "                 ON CASE"
                        + "                      WHEN ic.column_id IS NULL THEN fkc.parent_column_id"
                        + "                      ELSE ic.column_id"
                        + "                      END = c.column_id"
                        + "                   AND t.object_id = c.object_id"
                        + "       LEFT JOIN sys.columns fc"
                        + "                 ON fkc.referenced_column_id = fc.column_id"
                        + "                   AND f.referenced_object_id = fc.object_id"
                        + "       LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc"
                        + "                 ON rc.CONSTRAINT_NAME = o.name"
                        + " WHERE t.is_ms_shipped = 0 "
                        + " AND o.name IS NOT NULL"
                        + " AND o.type IN ('PK', 'UQ', 'F')";

        try (PreparedStatement stmt = c.prepareStatement(query);
                ResultSet results = stmt.executeQuery()) {

            Map<TableRef, Map<String, SqlServerKey>> tempKeyMap = new HashMap<>();

            while (results.next()) {
                String keyName = results.getString("KEY_NAME");
                String keyType = results.getString("KEY_TYPE");

                String columnName = results.getString("COLUMN_NAME");
                int ordinalPosition = FlagName.SqlServerOrderByPK.check() ? results.getInt("KEY_ORDINAL") : 0;

                int indexId = results.getInt("INDEX_ID");

                String refColumnName = results.getString("REFERENCED_COLUMN_NAME");

                String tableSchema = results.getString("TABLE_SCHEMA");
                String tableName = results.getString("TABLE_NAME");

                TableRef table = new TableRef(tableSchema, tableName);

                Map<String, SqlServerKey> tableKeys = tempKeyMap.computeIfAbsent(table, __ -> new HashMap<>());

                // We may need these if we construct a new SqlServerKey. Call this code
                // here so that if there is a SQLException we don't have to deal with it in the
                // computeIfAbsent below.
                String refTableSchema = results.getString("REFERENCED_SCHEMA");
                String refTableName = results.getString("REFERENCED_TABLE");

                SqlServerKey key =
                        tableKeys.computeIfAbsent(
                                keyName,
                                __ ->
                                        newSqlServerKeyFromResultSet(
                                                refTableSchema, refTableName, keyType, indexId, table));

                key.addColumn(columnName, ordinalPosition);
                key.addReferenceColumn(refColumnName);
            }

            return tempKeyMap
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue().values())));
        }
    }

    /**
     * @param refTableSchema
     * @param refTableName
     * @param keyType
     * @param indexId
     * @param table
     * @return
     */
    private static SqlServerKey newSqlServerKeyFromResultSet(
            String refTableSchema, String refTableName, String keyType, int indexId, TableRef table) {
        Optional<TableRef> refTable;
        if (refTableName == null || refTableSchema == null) {
            refTable = Optional.empty();
        } else {
            refTable = Optional.of(new TableRef(refTableSchema, refTableName));
        }

        return new SqlServerKey(table, keyType, indexId, refTable);
    }

    /** @return true if table present at source, otherwise false */
    boolean haveTablesAtSource() {
        return source.executeWithRetry(connection -> !getTablePermissionMap(connection).isEmpty());
    }

    private static Map<TableRef, TablePermissions> getTablePermissionMap(Connection c) throws SQLException {

        String query =
                "SELECT SCHEMA_NAME(st.schema_id) as TABLE_SCHEMA, st.name as TABLE_NAME, p.subentity_name, p.permission_name"
                        + " FROM sys.tables AS st"
                        + " CROSS APPLY sys.fn_my_permissions(QUOTENAME(SCHEMA_NAME(st.schema_id)) + N'.' + QUOTENAME(st.name), N'OBJECT') AS p"
                        + " WHERE p.permission_name IN ('VIEW CHANGE TRACKING', 'SELECT')"
                        + "  AND st.type = 'U'"
                        + "  AND st.is_ms_shipped = 0";

        try (PreparedStatement stmt = c.prepareStatement(query);
                ResultSet results = stmt.executeQuery()) {

            Map<TableRef, TablePermissions> tablePermissionMap = new HashMap<>();

            while (results.next()) {
                String schema = results.getString("TABLE_SCHEMA");
                String name = results.getString("TABLE_NAME");
                String column = results.getString("SUBENTITY_NAME");
                String permission = results.getString("PERMISSION_NAME");

                TableRef tableName = new TableRef(schema, name);
                TablePermissions tablePerms =
                        tablePermissionMap.computeIfAbsent(tableName, __ -> new TablePermissions());

                if (column != null && !column.isEmpty()) {
                    tablePerms.selectableColumns.add(column);
                } else if (permission.equals("VIEW CHANGE TRACKING")) {
                    tablePerms.viewChangedTrackingEnabled = true;
                }
            }

            return tablePermissionMap;
        }
    }

    static class CDCNotEnabledForTable extends RuntimeException {}

    static Set<String> getTableCDCColumns(Connection c, TableRef tableRef) throws SQLException {
        try {
            String query =
                    String.format(
                            "EXEC sys.sp_cdc_help_change_data_capture @source_schema = N'%s', @source_name = N'%s'",
                            tableRef.schema, tableRef.name);
            CDCInfo cdcInfo = getCDCInfo(c, query);
            if (!cdcInfo.isCDCEnabled) throw new CDCNotEnabledForTable();
            else return cdcInfo.columnMap.get(tableRef);
        } catch (SQLException e) {
            // CDC has not been enabled for source table.
            // See
            // https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-ver15#errors-22000-to-22999
            if (e.getErrorCode() == 22985) throw new CDCNotEnabledForTable();
            throw e;
        }
    }

    private static CDCInfo getCDCInfo(Connection c) throws SQLException {
        return getCDCInfo(c, "EXEC sys.sp_cdc_help_change_data_capture");
    }

    private static CDCInfo getCDCInfo(Connection c, String query) throws SQLException {
        // Skip if CDC is not enabled on current database
        if (!isCDCEnabled(c)) return new CDCInfo(false);

        try (PreparedStatement stmt = c.prepareStatement(query);
                ResultSet results = stmt.executeQuery()) {
            CDCInfo cdcInfo = new CDCInfo(true);

            while (results.next()) {
                String schema = results.getString("source_schema");
                String name = results.getString("source_table");

                /* A CDC instance can sometimes remain after a table is dropped
                 * so we need to do a null-check beforehand. */
                if (schema == null || name == null) continue;

                String captureInstance = results.getString("capture_instance");
                String capturedColumnList = results.getString("captured_column_list");

                TableRef table = new TableRef(schema, name);

                cdcInfo.addCaptureInstanceForTable(table, captureInstance);

                /* `captured_column_list` has this format: "[column_1], [column_2], [column_3] ... [column_last]"
                 * so we split by comma and remove the first & last brackets
                 */
                Set<String> capturedColumns =
                        Arrays.stream(capturedColumnList.split(", "))
                                .map(col -> col.substring(1, col.length() - 1))
                                .collect(Collectors.toSet());
                cdcInfo.columnMap.put(table, capturedColumns);
            }

            return cdcInfo;
        }
    }

    static boolean isCDCEnabled(Connection c) throws SQLException {
        // Query to check if CDC is enabled on current database
        try (ResultSet r =
                c.prepareStatement("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").executeQuery()) {
            return r.next() && r.getBoolean(1);
        }
    }

    private static Map<TableRef, Long> getCTTableSet(Connection c) throws SQLException {

        String query =
                "SELECT OBJECT_SCHEMA_NAME(object_id) as TABLE_SCHEMA,"
                        + " OBJECT_NAME(object_id) as TABLE_NAME,"
                        + " min_valid_version"
                        + " FROM sys.change_tracking_tables";

        try (PreparedStatement stmt = c.prepareStatement(query);
                ResultSet results = stmt.executeQuery()) {

            Map<TableRef, Long> ctTables = new HashMap<>();

            while (results.next()) {
                String schema = results.getString("TABLE_SCHEMA");
                String name = results.getString("TABLE_NAME");
                long minChangeVersion = results.getLong("min_valid_version");

                TableRef tableName = new TableRef(schema, name);
                ctTables.put(tableName, minChangeVersion);
            }

            return ctTables;
        }
    }

    Map<TableRef, Set<RawColumnInfo>> getRawColumnInformationMap(Connection c) throws SQLException {

        String query =
                "SELECT"
                        + "       OBJECT_SCHEMA_NAME(sc.object_id)                 as TABLE_SCHEMA,"
                        + "       OBJECT_NAME(sc.object_id)                        as TABLE_NAME,"
                        + "       sc.name                                          as COLUMN_NAME,"
                        + "       sc.column_id,"
                        + "       ISNULL(TYPE_NAME(sc.system_type_id), t.name)      as DATA_TYPE,"
                        + "       COLUMNPROPERTY(sc.object_id, sc.name, 'ordinal') as ORDINAL_POSITION,"
                        + "       ISNULL(TYPE_NAME(sc.system_type_id), t.name)     as IS_NULLABLE,"
                        + "       COLUMNPROPERTY(sc.object_id, sc.name, 'octetmaxlen') as CHARACTER_OCTET_LENGTH,"
                        + "       convert(tinyint, CASE WHEN sc.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN sc.precision END) as NUMERIC_PRECISION,"
                        + "       convert(int, CASE WHEN sc.system_type_id IN (40, 41, 42, 43, 58, 61) THEN NULL ELSE ODBCSCALE(sc.system_type_id, sc.scale) END) as NUMERIC_SCALE"
                        + " FROM sys.columns AS sc"
                        + "         LEFT JOIN sys.types t ON sc.user_type_id = t.user_type_id"
                        + "         LEFT JOIN sys.tables as tbs"
                        + "                   ON sc.object_id = tbs.object_id"
                        + " WHERE tbs.is_ms_shipped = 0"
                        + " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

        try (PreparedStatement stmt = c.prepareStatement(query);
                ResultSet results = stmt.executeQuery()) {

            Map<TableRef, Set<RawColumnInfo>> rawColumnInfoMap = new HashMap<>();

            while (results.next()) {
                String schema = results.getString("TABLE_SCHEMA");
                String name = results.getString("TABLE_NAME");

                RawColumnInfo rawColumnInfo = new RawColumnInfo();

                rawColumnInfo.columnName = results.getString("COLUMN_NAME");
                rawColumnInfo.ordinalPosition = results.getInt("ORDINAL_POSITION");

                String sourceType = results.getString("DATA_TYPE");
                rawColumnInfo.sourceType = SqlServerType.valueOf(sourceType.toUpperCase());
                rawColumnInfo.destinationType = SqlServerType.destinationTypeOf(rawColumnInfo.sourceType);

                rawColumnInfo.byteLength =
                        Optional.ofNullable(results.getString("CHARACTER_OCTET_LENGTH"))
                                .map(Long::valueOf)
                                // ensure byte max length does not exceed integer max value
                                .map(longValue -> (int) Math.min(longValue, Integer.MAX_VALUE))
                                .map(OptionalInt::of)
                                .orElseGet(OptionalInt::empty);

                rawColumnInfo.columnIndex = getOptionalInt(results, "COLUMN_ID");
                rawColumnInfo.numericPrecision = getOptionalInt(results, "NUMERIC_PRECISION");
                rawColumnInfo.numericScale = getOptionalInt(results, "NUMERIC_SCALE");
                rawColumnInfo.isNullable = results.getString("IS_NULLABLE");

                TableRef table = new TableRef(schema, name);
                Set<RawColumnInfo> rawColumns = rawColumnInfoMap.computeIfAbsent(table, __ -> new HashSet<>());
                rawColumns.add(rawColumnInfo);
            }

            return rawColumnInfoMap;
        }
    }

    /**
     * This method is called repeatedly by the front end via . Because caching the results of {@link DbInformer#allTables()} is not
     * possible in this case, we must use a lighter weight query.
     */
    public Map<String, ColumnConfigInformation> fetchColumnConfigInfo(TableRef tableWithSchema) {
        String query =
                "SELECT sc.name AS COLUMN_NAME,"
                        + "       ISNULL(TYPE_NAME(sc.system_type_id), t.name) AS DATA_TYPE,"
                        + "       i.is_primary_key"
                        + " FROM sys.indexes AS i"
                        + "         INNER JOIN"
                        + "     sys.index_columns AS ic ON"
                        + "             i.OBJECT_ID = ic.OBJECT_ID AND"
                        + "             i.index_id = ic.index_id AND i.is_primary_key = 'true'"
                        + "         RIGHT JOIN sys.columns AS sc ON"
                        + "    sc.object_id = ic.object_id AND sc.column_id = ic.column_id"
                        + "         LEFT JOIN sys.types t ON sc.user_type_id = t.user_type_id"
                        + " WHERE sc.object_id = OBJECT_ID(?)"
                        + " ORDER BY sc.column_id";

        return source.executeWithRetry(
                connection -> {
                    Set<String> cdcColumns = new HashSet<>();
                    boolean isCdcEnabled = false;
                    if (!isCTEnabledTable(connection, tableWithSchema)) {
                        try {
                            cdcColumns.addAll(getTableCDCColumns(connection, tableWithSchema));
                            isCdcEnabled = true;
                        } catch (CDCNotEnabledForTable e) {
                            isCdcEnabled = false;
                        }
                    }

                    try (PreparedStatement selectColumns = connection.prepareStatement(query)) {
                        selectColumns.setString(1, tableWithSchema.toString());

                        try (ResultSet columnInfo = selectColumns.executeQuery()) {

                            Map<String, ColumnConfigInformation> columns = new HashMap<>();

                            while (columnInfo.next()) {
                                String columnName = columnInfo.getString("COLUMN_NAME");
                                String columnType = columnInfo.getString("DATA_TYPE");

                                SqlServerType sqlServerType = SqlServerType.valueOf(columnType.toUpperCase());
                                boolean isSupported = SqlServerType.destinationTypeOf(sqlServerType).isPresent();
                                boolean isPrimaryKey = columnInfo.getBoolean("is_primary_key");

                                if (!isSupported) {
                                    columns.put(columnName, new ColumnConfigInformation(isPrimaryKey, false));
                                } else if (isCdcEnabled && !cdcColumns.contains(columnName)) {
//                                    columns.put(
//                                            columnName,
//                                            new ColumnConfigInformation(
//                                                    isPrimaryKey,
//                                                    false,
//                                                    Optional.of("Column not included in CDC instance")));
                                } else {
                                    columns.put(columnName, new ColumnConfigInformation(isPrimaryKey, true));
                                }
                            }

                            if (columns.isEmpty()) {
                                LOG.warning(
                                        "The source table "
                                                + tableWithSchema.schema
                                                + "."
                                                + tableWithSchema.name
                                                + " does not exist ");
                            }
                            return columns;
                        }
                    }
                });
    }

    private static boolean isCTEnabledTable(Connection c, TableRef tableRef) throws SQLException {
        String isCTEnabledSql =
                "SELECT 1 FROM sys.change_tracking_tables "
                        + " WHERE OBJECT_SCHEMA_NAME(object_id) = '"
                        + tableRef.schema
                        + "' "
                        + " AND OBJECT_NAME(object_id) = '"
                        + tableRef.name
                        + "'";

        try (PreparedStatement statement = c.prepareStatement(isCTEnabledSql);
                ResultSet rs = statement.executeQuery()) {
            return rs.next();
        }
    }

    private static OptionalInt getOptionalInt(ResultSet r, String column) throws SQLException {
        int maybeInt = r.getInt(column);
        return r.wasNull() ? OptionalInt.empty() : OptionalInt.of(maybeInt);
    }

    private static class TablePermissions {
        boolean viewChangedTrackingEnabled;
        final Set<String> selectableColumns = new HashSet<>();
    }

    private static class CDCInfo {
        final boolean isCDCEnabled;
        final Map<TableRef, Set<String>> columnMap = new HashMap<>();
        final Map<TableRef, Set<String>> captureInstanceMap = new HashMap<>();

        CDCInfo(boolean isCDCEnabled) {
            this.isCDCEnabled = isCDCEnabled;
        }

        void addCaptureInstanceForTable(TableRef table, String captureInstance) {
            captureInstanceMap.computeIfAbsent(table, __ -> new HashSet<>()).add(captureInstance);
        }
    }

    /**
     * Contains and manages {@link RawTableInfo} and column meta-data aggregations, which are used to convert {@link
     * RawTableInfo} into {@link SqlServerTableInfo}.
     */
    private class RawTableInfo {
        TableRef sourceTable;
        boolean canViewChangeTracking;
        boolean hasPrimaryKey;
        boolean hasClusteredIndex;
        int originalColumnCount;
        long estimatedRowCount;
        long estimatedDataBytes;
        SqlServerChangeType changeType;
        String cdcCaptureInstance;
        Set<ExcludeReason> excludeReasons = new HashSet<>();
        List<SqlServerKey> keys = new ArrayList<>();
        List<SqlServerColumnInfo> columns = new ArrayList<>();
        Long minChangeVersion = null;
        SyncMode syncMode;

        RawTableInfo(TableRef tableRef) {
            this.sourceTable = tableRef;
        }

        Set<String> getColumnNames() {
            return columns.stream().map(col -> col.columnName).collect(Collectors.toSet());
        }

        // TODO: Figure out a way to calculate the row count and # of bytes per table
        SqlServerTableInfo convertToTableInfo() {
            return new SqlServerTableInfo(
                    sourceTable,
                    source.params.schema,
                    estimatedRowCount,
                    estimatedDataBytes,
                    columns,
                    originalColumnCount,
                    excludeReasons,
                    changeType,
                    cdcCaptureInstance,
                    hasClusteredIndex,
                    minChangeVersion,
                    syncMode);
        }
    }

    /**
     * Contains basic meta-data for a given source column that does not require aggregation (it is directly available).
     */
    static class RawColumnInfo {
        String columnName;
        int ordinalPosition;
        SqlServerType sourceType;
        Optional<DataType> destinationType;
        OptionalInt byteLength;
        OptionalInt columnIndex;
        OptionalInt numericPrecision;
        OptionalInt numericScale;
        String isNullable;

        RawColumnInfo() {}
    }
}
