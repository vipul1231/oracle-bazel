package com.example.oracle;

import com.example.core.TableRef;
import com.example.db.DbQuery;
import com.example.flag.FeatureFlag;
import com.example.lambda.Lazy;
import com.example.logger.ExampleLogger;
import com.example.logger.event.integration.InfoEvent;
import com.example.oracle.cache.PartitionedTableCache;
import com.example.oracle.cache.TableRowCountCache;
import com.example.oracle.exceptions.QuickBlockRangeFailedException;
import com.example.oracle.flashback.FlashbackSetupValidator;
import com.example.oracle.logminer.LogMinerSession;
import com.example.oracle.logminer.LogMinerSetupValidator;
import com.google.common.collect.ImmutableSet;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.example.oracle.Constants.*;
import static com.example.oracle.OracleErrorCode.NUMERIC_OVERFLOW;
import static com.example.oracle.OracleErrorCode.ORA_08181;
import static com.example.oracle.SqlUtil.buildSchemaFilter;
import static com.example.oracle.SqlUtil.scnXHoursBeforeScn;
import static com.example.oracle.Transactions.RetryFunction;
import static com.example.oracle.Transactions.hasDbaSegmentsAccess;
import static com.example.oracle.Util.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleApi implements AutoCloseable {

    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    private OracleOutputHelper outputHelper;
    TableRowCountCache tableRowCountCache;

    private FlashbackSetupValidator flashbackSetupValidator;
    private LogMinerSetupValidator logMinerSetupValidator;
    private DbaExtendsHandler dbaExtendsHandler;

    private DatabaseDao databaseDao;
    private WarningIssuer warningIssuer = warning -> {};

    private ConnectionFactory connectionFactory;

//    Lazy<OracleDynamicPageSizeConfig> pageSizeConfig = new Lazy<OracleDynamicPageSizeConfig>(this::getPageSizeConfig);
      Lazy<OracleDynamicPageSizeConfig> pageSizeConfig = null;

    public OracleApi(DataSource dataSource) {
        this.connectionFactory = ConnectionFactory.init(dataSource);

        // TODO, check when to initialize. Is it a stitic map or is it for transaction ?
        tableRowCountCache = new TableRowCountCache();
        databaseDao = new DatabaseDao();

        flashbackSetupValidator = new FlashbackSetupValidator();
        dbaExtendsHandler = new DbaExtendsHandler();
    }

    public void setOutputHelper(OracleOutputHelper outputHelper) {
        if (this.outputHelper == null) {
            this.outputHelper = outputHelper;
        }
    }

    public Connection connectToSourceDb() {
        return connectionFactory.connectToSourceDb();
    }

    /**@TODO: Need to write logic to get sourceDB based on container type
     *
     * @param container
     * @return
     */
    public Connection connectToSourceDb(Container container) {
        return connectionFactory.connectToSourceDb();
    }

    @Override
    public void close() {
        ConnectionFactory.getInstance().close();
    }

    public Set<TableRef> partitionedTablesWithRowMovementEnabled() {
        return PartitionedTableCache.partitionedTablesWithRowMovementEnabled();
    }

    void setWarningIssuer(WarningIssuer newIssuer) {
        this.warningIssuer = newIssuer;
    }

    /**
     * Names of all tables
     */
    public Map<TableRef, Optional<String>> tables() {
        return databaseDao.tables();
    }

    String getParallelHint(TableRef table) {
        long rowCount;
        try {
            rowCount = tableRowCountCache.getRowCount(table);
        } catch (Exception e) {
            // Ignore the exception
            LOG.warning("Error while getting row count for " + table);
            // Set a higher number as if we could not get rowCount, probably the table is problematic
            rowCount = VERSION_QUERY_PARALLEL_HINT_MIN_ROW_COUNT;
        }

        return rowCount < VERSION_QUERY_PARALLEL_HINT_MIN_ROW_COUNT
                ? ""
                : "/*+ parallel(" + table.name + "," + VERSION_QUERY_PARALLEL_HINT_DEGREE + ")*/ ";
    }

    @SuppressWarnings("java:S2095")
    PreparedStatement getLastDmlScnForTable(Connection connection, TableRef table) throws SQLException {
        String tableString = doubleQuote(table);

//        @Language("SQL")
        String query = "SELECT MAX(ORA_ROWSCN) FROM " + tableString;

        return connection.prepareStatement(query);
    }

    Set<String> getTablespaces(TableRef table) throws QuickBlockRangeFailedException {
        return getTablespaces(ImmutableSet.of(table)).get(table);
    }

    /**
     * Determines which tablespaces a set of tables belong to. Tables can have a few configurations: 1. Non-partitioned
     * table. These always reside in a single tablespace. 2. Partitioned table in a single tablespace. 3. Partitioned
     * table in multiple tablespaces.
     *
     * <p>The view ALL_TABLES will get us the tablespace for config #1. It will also tell us if a table is Partitioned.
     * The view DBA_SEGMENTS will get us the tablespaces for config #2 and #3.
     *
     * <p>NOTE: While we only ever call this for one table at a time right now, I want to keep this Set based method for
     * when we convert to using DbInformer.
     */
    Map<TableRef, Set<String>> getTablespaces(Set<TableRef> tables) throws QuickBlockRangeFailedException {
        return connectionFactory.retry(
                "get tablespaces",
                e -> new QuickBlockRangeFailedException("Failed to get tablespaces", e),
                connection -> {
                    Map<TableRef, Set<String>> tablespaces = new HashMap<>();
                    Set<TableRef> partitionedTables = new HashSet<>();

//                    @Language("SQL")
                    String query =
                            "SELECT OWNER, TABLE_NAME, TABLESPACE_NAME, PARTITIONED FROM ALL_TABLES WHERE OWNER IN ("
                                    + tables.stream().map(t -> singleQuote(t.schema)).collect(Collectors.joining(","))
                                    + ") AND TABLE_NAME IN ("
                                    + tables.stream().map(t -> singleQuote(t.name)).collect(Collectors.joining(","))
                                    + ")";

                    try (PreparedStatement statement = connection.prepareStatement(query);
                         ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            String owner = result.getString("OWNER");
                            String tableName = result.getString("TABLE_NAME");
                            String tablespaceName = result.getString("TABLESPACE_NAME");
                            boolean partitioned = result.getString("PARTITIONED").equals("YES");

                            TableRef table = new TableRef(owner, tableName);

                            if (partitioned) partitionedTables.add(table);

                            tablespaces.compute(
                                    table,
                                    (__, set) -> {
                                        if (set == null) set = new HashSet<>();
                                        if (tablespaceName != null) set.add(tablespaceName);
                                        return set;
                                    });
                        }
                    }

                    /*
                     * Partitioned tables have a one-to-many relationship with their tablespaces. We need to look those
                     * tablespaces up in another view.
                     */
                    if (!partitionedTables.isEmpty()) {

                        if (!hasDbaSegmentsAccess.get()) {
                            throw new QuickBlockRangeFailedException(
                                    "Need DBA_SEGMENTS access to quickly import partitioned tables");
                        }

//                        @Language("SQL")
                        String partQuery =
                                "SELECT OWNER, SEGMENT_NAME, TABLESPACE_NAME FROM DBA_SEGMENTS WHERE OWNER IN ("
                                        + partitionedTables
                                        .stream()
                                        .map(t -> singleQuote(t.schema))
                                        .collect(Collectors.joining(","))
                                        + ") AND SEGMENT_NAME IN ("
                                        + partitionedTables
                                        .stream()
                                        .map(t -> singleQuote(t.name))
                                        .collect(Collectors.joining(","))
                                        + ")";

                        try (PreparedStatement statement = connection.prepareStatement(partQuery);
                             ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                String owner = result.getString("OWNER");
                                String tableName = result.getString("SEGMENT_NAME");
                                String tablespaceName = result.getString("TABLESPACE_NAME");

                                TableRef table = new TableRef(owner, tableName);

                                tablespaces.compute(
                                        table,
                                        (__, set) -> {
                                            if (set == null) set = new HashSet<>();
                                            if (tablespaceName != null) set.add(tablespaceName);
                                            return set;
                                        });
                            }
                        }
                    }

                    return tablespaces;
                });
    }


    /**
     * Converts the given SCN to its timestamp representation. If the given SCN is not valid (too far in the past),
     * Instant.EPOCH is returned.
     */
    public Instant convertScnToTimestamp(long scn) {
//        @Language("SQL")
        String query = "SELECT SCN_TO_TIMESTAMP(?) AS SCN_TIMESTAMP FROM DUAL";

        RetryFunction<Instant> action =
                (connection) -> {
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        statement.setBigDecimal(1, new BigDecimal(scn));

                        try (ResultSet result = statement.executeQuery()) {
                            if (!result.next()) throw new RuntimeException("No result for converting scn to timestamp");

                            return result.getTimestamp("SCN_TIMESTAMP").toInstant();
                        }
                    } catch (SQLException e) {
                        if (ORA_08181.is(e)) { // specified number is not a valid system change number
                            return Instant.EPOCH;
                        }

                        throw e;
                    }
                };

        return connectionFactory.retry(
                "convertScnToTimestamp", t -> new RuntimeException("Error in converting scn to timestamp", t), action);
    }

    public boolean isInvalidRowId(String rowId) {
        return INVALID_ROWID.matcher(rowId).matches();
    }

    public RowMap buildRow(List<OracleColumn> columns, Transactions.RowValues rowValues) throws SQLException {
        String[] keys = new String[columns.size()], values = new String[columns.size()];
        boolean[] present = new boolean[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            String colName = "c" + i;
            Object rawValue = rowValues.get(colName);
            String stringValue = (rawValue != null) ? rawValue.toString() : null;
            OracleColumn col = columns.get(i);
            keys[i] = col.name;

            if (stringValue != null) {
                // Originally, we trimmed whitespace on all values. With this flag, we trim only non-String-like values.
                if (!FeatureFlag.check("OracleLeaveTrailingWhitespace") || !col.oracleType.isStringLike()) {
                    stringValue = stringValue.trim();
                }
                if (stringValue.equals(SKIP_VALUE)) {
                    continue;
                }
            }
            values[i] = stringValue;
            present[i] = true;
        }

        return new RowMap(keys, values, present);
    }

    public Optional<LogMinerOperation> getLatestTransactionStatus(String xId, Long start) {
        // Get the latest SCN
        long end = databaseDao.getCurrentScn();
        LOG.info("Getting Logminer transaction status for Id: " + xId + " SCN start: " + start + " SCN end: " + end);
        try (Connection connection = connectToSourceDb();
             LogMinerSession logMinerSession =
                     new LogMinerSession(connection).advancedOptions().scnSpan(start, end);) {
            logMinerSession.start();
            return logMinerSession.getLogMinerContentsDao().getTransactionStatus(xId);
        } catch (SQLException e) {
            LOG.warning("Exception while getting Logminer transaction status for Id:" + xId);
            throw new RuntimeException("Exception while getting Logminer transaction status for Id:" + xId, e);
        }
    }

    private Map<TableRef, HashSet<String>> getConstraints(Set<TableRef> selected) {
        LOG.info("Getting column keys for all selected tables");

        Map<TableRef, HashSet<String>> tableConstraints = new HashMap<>();
        String schemaFilter = buildSchemaFilter(selected);

        // We order the resultset so that we pick the same one of potentially many foreign key constraints every time
        // Performance hit of ordering the resultset is negligible because number of constraints are small

//        @Language("SQL")
        String query =
                "SELECT cnst.owner, cnst.table_name, cons.column_name, cnst.constraint_name, cnst.constraint_type "
                        + "FROM all_constraints cnst "
                        + " INNER JOIN all_cons_columns cons "
                        + "   ON cnst.owner = cons.owner and cnst.table_name = cons.table_name and cnst.constraint_name = cons.constraint_name "
                        + "WHERE cnst.status != 'DISABLED' and cnst.CONSTRAINT_TYPE in ('P', 'R') "
                        + "AND "
                        + "cnst.owner "
                        + schemaFilter
                        + "ORDER BY cnst.constraint_name, cons.column_name";

        RetryFunction<Map<TableRef, HashSet<String>>> action =
                (connection) -> {

                    // constraint names are unique
                    HashSet<String> excludedConstraints = new HashSet<>();
                    // schema.table.column, constraint name (we allow maximum of one P and one F per column)
                    Map<String, HashSet<KeyType>> columnConstraints = new HashMap<>();

                    try (Statement selectColumns = connection.createStatement();
                         ResultSet r = selectColumns.executeQuery(query)) {

                        r.setFetchSize(DEFAULT_FETCH_SIZE);

                        while (r.next()) {
                            String schema = r.getString("owner");
                            String table = r.getString("table_name");
                            String column = r.getString("column_name");
                            String constraintName = r.getString("constraint_name");
                            KeyType constraintType = convertToKeyType(r.getString("constraint_type"));

                            TableRef tableRef = new TableRef(schema, table);
                            String fullColumn = Joiner.on(".").join(schema, table, column);

                            tableConstraints.putIfAbsent(tableRef, new HashSet<>());

                            if (columnConstraints.containsKey(fullColumn)) {
                                HashSet<KeyType> existingColumnConstraints = columnConstraints.get(fullColumn);

                                if (existingColumnConstraints.contains(constraintType)) {
                                    if (constraintType == KeyType.PRIMARY) {
                                        // this should never happen
                                        throw new RuntimeException(
                                                "Duplicate primary key on column: " + column + " table: " + tableRef);
                                    } else if (constraintType == KeyType.FOREIGN) {
                                        // TODO: We should eventually allow multiple foreign keys (and foreign keys
                                        // involving
                                        // multiple columns) in the core
                                        LOG.info(
                                                "Ignoring other foreign key on table: "
                                                        + tableRef
                                                        + " column: "
                                                        + column);
                                        excludedConstraints.add(constraintName);
                                    }
                                } else {
                                    existingColumnConstraints.add(constraintType);
                                    tableConstraints.get(tableRef).add(constraintName);
                                }

                            } else {
                                if (!excludedConstraints.contains(constraintName)) {
                                    columnConstraints.put(fullColumn, new HashSet<>());
                                    columnConstraints.get(fullColumn).add(constraintType);

                                    tableConstraints.get(tableRef).add(constraintName);
                                }
                            }
                        }

                        return tableConstraints;
                    }
                };

        return connectionFactory.retry(
                "getConstraints",
                t -> new RuntimeException("Error in getting column keys for all selected tables", t),
                action);
    }

    Map<String, ColumnConfigInformation> tableColumns(TableRef tableWithSchema) {
        LOG.info("Getting column info for table " + tableWithSchema.schema + "." + tableWithSchema.name);
        Map<String, ColumnConfigInformation> columns = new HashMap<>();

//        @Language("SQL")
        String query =
                "SELECT "
                        + "  tabs.owner, tabs.table_name, tabs.column_name, "
                        + "  tabs.data_type "
                        + "  ,jcons.column_name, jcons.constraint_name, jcons.constraint_type "
                        + "FROM all_tab_columns tabs "
                        + "  LEFT JOIN (select cons.owner, cons.table_name, cons.column_name "
                        + "                   ,cnst.constraint_name, cnst.constraint_type, cnst.r_constraint_name "
                        + "             from all_cons_columns cons "
                        + "             left join all_constraints cnst "
                        + "                 on cons.owner = cnst.owner and cons.table_name = cnst.table_name and cons.constraint_name = cnst.constraint_name "
                        + "             where cnst.status != 'DISABLED' and cnst.CONSTRAINT_TYPE in ('P', 'R')) jcons "
                        + "       ON tabs.owner = jcons.owner and tabs.table_name = jcons.table_name and tabs.column_name = jcons.column_name "
                        + "WHERE tabs.owner = '"
                        + tableWithSchema.schema
                        + "' AND tabs.table_name = '"
                        + tableWithSchema.name
                        + "'";

        RetryFunction<Map<String, ColumnConfigInformation>> action =
                (connection) -> {
                    try (Statement selectColumns = connection.createStatement();
                         ResultSet columnInfo = selectColumns.executeQuery(query)) {
                        columnInfo.setFetchSize(DEFAULT_FETCH_SIZE);

                        while (columnInfo.next()) {
                            String columnName = columnInfo.getString("column_name");
                            String columnType = columnInfo.getString("data_type");
                            boolean isSupported = OracleType.create(columnType).getWarehouseType().isPresent();

                            Optional<String> constraintType =
                                    Optional.ofNullable(columnInfo.getString("constraint_type"));
                            boolean isPrimaryKey = "P".equals(constraintType.orElse(null));
                            columns.put(columnName, new ColumnConfigInformation(isPrimaryKey, isSupported));
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
                };

        return connectionFactory.retry("tableColumns", t -> new RuntimeException("Error while getting column info", t), action);
    }

    Map<TableRef, List<OracleColumn>> columns(Set<TableRef> selected) {
        LOG.info("Getting column info");

        // List of key names to process per table (any others will be skipped because they could be multiple
        // foreign keys on the same column (single or multi-column) or constraint types we don't care about)
        Map<TableRef, HashSet<String>> tableConstraints = getConstraints(selected);

        String schemaFilter = buildSchemaFilter(selected);

//        @Language("SQL")
        String query =
                "SELECT "
                        + "  tabs.owner, tabs.table_name, tabs.column_name, tabs.column_id "
                        + "  ,tabs.data_type,tabs.char_col_decl_length, tabs.data_precision, tabs.data_scale "
                        + "  ,jcons.column_name, jcons.constraint_name, jcons.constraint_type "
                        + "  ,jcons.r_constraint_name "
                        + "  ,ref_cons.constraint_name as referenced_constraint_name "
                        + "  ,ref_cons.owner as referenced_schema, ref_cons.table_name as referenced_table "
                        + "FROM all_tab_columns tabs "
                        + "  LEFT JOIN (select cons.owner, cons.table_name, cons.column_name "
                        + "                   ,cnst.constraint_name, cnst.constraint_type, cnst.r_constraint_name "
                        + "             from all_cons_columns cons "
                        + "             left join all_constraints cnst "
                        + "                 on cons.owner = cnst.owner and cons.table_name = cnst.table_name and cons.constraint_name = cnst.constraint_name "
                        + "             where cnst.status != 'DISABLED' and cnst.CONSTRAINT_TYPE in ('P', 'R')) jcons "
                        + "       ON tabs.owner = jcons.owner and tabs.table_name = jcons.table_name and tabs.column_name = jcons.column_name "
                        + "  LEFT join all_constraints ref_cons on "
                        + (FeatureFlag.check("OracleFilterNullConstraintInColumnQuery")
                        ? "jcons.r_constraint_name IS NOT NULL and "
                        : "")
                        + "ref_cons.constraint_name = jcons.r_constraint_name "
                        + "WHERE tabs.owner "
                        + schemaFilter;

        RetryFunction<Map<TableRef, List<OracleColumn>>> action =
                (connection) -> {
                    try (DbQuery selectColumns = new DbQuery(connection.createStatement());
                         ResultSet columnInfo = selectColumns.executeQuery(query)) {
                        columnInfo.setFetchSize(DEFAULT_FETCH_SIZE);

                        Map<TableRef, Map<String, OracleColumn>> allColumns = new HashMap<>();

                        while (columnInfo.next()) {
                            String schema = columnInfo.getString("owner");
                            String table = columnInfo.getString("table_name");
                            TableRef tableRef = new TableRef(schema, table);

                            if (!selected.isEmpty() && !selected.contains(tableRef)) {
                                continue;
                            }

                            String columnName = columnInfo.getString("column_name");
                            Optional<String> constraintType =
                                    Optional.ofNullable(columnInfo.getString("constraint_type"));
                            String constraintName = columnInfo.getString("constraint_name");

                            allColumns.putIfAbsent(tableRef, new HashMap<>());

                            if (allColumns.get(tableRef).containsKey(columnName)) {
                                if (constraintType.isPresent()
                                        && tableConstraints.get(tableRef).contains(constraintName)) {
                                    switch (constraintType.get()) {
                                        case "P":
                                            allColumns
                                                    .get(tableRef)
                                                    .computeIfPresent(columnName, (__, col) -> col.withPrimaryKey());
                                            break;

                                        case "R":
                                            Optional<TableRef> referencedTable;
                                            String refConstraint = columnInfo.getString("referenced_constraint_name");
                                            if (refConstraint == null) {
                                                // When a referenced table is dropped, Oracle only removes the
                                                // referenced part of the constraint, without removing the
                                                // referencing constraint. Therefore we can get a foreign key
                                                // constraint with a missing target.
                                                referencedTable = Optional.empty();
                                            } else {
                                                String refSchema = columnInfo.getString("referenced_schema");
                                                String refTable = columnInfo.getString("referenced_table");
                                                referencedTable = Optional.of(new TableRef(refSchema, refTable));
                                            }
                                            allColumns
                                                    .get(tableRef)
                                                    .computeIfPresent(
                                                            columnName,
                                                            (__, col) -> col.withForeignKey(referencedTable));
                                            break;

                                        default:
                                            throw new RuntimeException(
                                                    "Unexpected constraint type: " + constraintType.get());
                                    }
                                }
                            } else {
                                boolean isPrimaryKey = false;

                                String columnType = columnInfo.getString("data_type");
                                // getBigDecimal returns `null` for SQL NULL. getInt does not!
                                BigDecimal byteLength = columnInfo.getBigDecimal("char_col_decl_length");
                                BigDecimal precision = columnInfo.getBigDecimal("data_precision");
                                BigDecimal scale = columnInfo.getBigDecimal("data_scale");

                                OracleType oracleType = OracleType.create(columnType, byteLength, precision, scale, 0, false, false);

                                if (!oracleType.getWarehouseType().isPresent()) {
                                    continue;
                                }

                                Optional<TableRef> referencedTable = Optional.empty();
                                if (constraintType.isPresent()
                                        && tableConstraints.get(tableRef).contains(constraintName)) {
                                    switch (constraintType.get()) {
                                        case "P":
                                            isPrimaryKey = true;
                                            break;

                                        case "R":
                                            String refConstraint = columnInfo.getString("referenced_constraint_name");
                                            if (refConstraint != null) {
                                                String refSchema = columnInfo.getString("referenced_schema");
                                                String refTable = columnInfo.getString("referenced_table");
                                                referencedTable = Optional.of(new TableRef(refSchema, refTable));
                                            }
                                            break;

                                        default:
                                            throw new RuntimeException(
                                                    "Unexpected constraint type: " + constraintType.get());
                                    }
                                }

                                OracleColumn col =
                                        new OracleColumn(
                                                columnName, oracleType, isPrimaryKey, tableRef, referencedTable);

                                allColumns.get(tableRef).put(columnName, col);
                            }
                        }

                        Map<TableRef, List<OracleColumn>> finalColumns = new HashMap<>();
                        for (TableRef tableRef : allColumns.keySet()) {
                            List<OracleColumn> origCols = new ArrayList<>(allColumns.get(tableRef).values());

                            // Include tables only if they have at least one column with a supported data type
                            if (origCols.size() > 0) finalColumns.put(tableRef, origCols);
                        }

                        return finalColumns;
                    }
                };

        return connectionFactory.retry("columns", t -> new RuntimeException("Error while getting column info", t), action);
    }

    PreparedStatement getLastTableChangeTimestamps(Connection connection, Collection<TableRef> included)
            throws SQLException {
        String tablesString = included.stream().map(c -> singleQuote(c.name)).collect(Collectors.joining(","));
        Set<String> ownerSet = included.stream().map(c -> singleQuote(c.schema)).collect(Collectors.toSet());
        String ownerString = String.join(",", ownerSet);

//        @Language("SQL")
        String query =
                "SELECT OWNER, OBJECT_NAME, TIMESTAMP FROM ALL_OBJECTS "
                        + "WHERE OBJECT_NAME IN ( "
                        + tablesString
                        + " ) "
                        + "AND OWNER IN ( "
                        + ownerString
                        + " )";
        return connection.prepareStatement(query);
    }

    public int getUndoLogRetention(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet r =
                     statement.executeQuery("SELECT value FROM V$SYSTEM_PARAMETER WHERE NAME = 'undo_retention'")) {
            if (r.next()) {
                return r.getInt("VALUE");
            } else {
                throw new RuntimeException("No value set for UNDO_RETENTION");
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("table or view does not exist"))
                throw new RuntimeException("Missing SELECT permission for V$SYSTEM_PARAMETER", e);
            throw new RuntimeException("Could not select UNDO_RETENTION: " + e.getMessage(), e);
        }
    }

    public void checkPermissions() throws SQLException {
        try (Connection connection = connectToSourceDb();
             LogMinerSession logMinerSession = new LogMinerSession(connection);) {
            LogMinerSetupValidator.validate(logMinerSession);
        }
    }


    /**
     * The most recent system-change-number, committed or not
     */
    public long mostRecentScn() {
        RetryFunction<Long> action = (connection) -> new LogMinerSession(connection).getArchivedLogDao().getEndScn();

        return connectionFactory.retry("mostRecentScn", t -> new RuntimeException("Error in getting most recent SCN", t), action);
    }

    /**
     * Problem: Every oracle server has a different "weight" to their SCNs. "weight" meaning that 1 SCN can encapsulate
     * a single statement or thousands. Since every server is unique, we need to figure out a baseline number of SCNs to
     * use for each page.
     *
     * <p>Solution: Get the average number of SCNs per minute over the last X hours. Then use that avg to compute a the
     * starting page size and reasonable page size adjustment values.
     *
     * <p>Using a large `PAGE_SIZE_CONFIG_HOURS` should help the config avoid being overly affected by outlier days.
     */
    OracleDynamicPageSizeConfig getPageSizeConfig() {
        // get current SCN
        long mostRecentScn = mostRecentScn();

        // get scn `PAGE_SIZE_CONFIG_HOURS` ago
        Optional<Long> previousScn = scnXHoursBeforeScn(PAGE_SIZE_CONFIG_HOURS, mostRecentScn);

        if (!previousScn.isPresent()) {
            // use default values
            return new OracleDynamicPageSizeConfig();
        }

        long scnsPerMin = (mostRecentScn - previousScn.get()) / (PAGE_SIZE_CONFIG_HOURS * 60L);

        // starting point should be ~30 mins
        // min should be ~5 mins
        // step should be ~5 mins
        // failure should be ~20 mins
        return new OracleDynamicPageSizeConfig(scnsPerMin * 30, scnsPerMin * 5, scnsPerMin * 5);
    }

    public TableRowCountCache getTableRowCountCache() {
        return tableRowCountCache;
    }

    public DatabaseDao getDatabaseDao() {
        return databaseDao;
    }

    public FlashbackSetupValidator getFlashbackSetupValidator() {
        return flashbackSetupValidator;
    }


    public LogMinerSetupValidator getLogMinerSetupValidator() {
        return logMinerSetupValidator;
    }

    void acceptRow(TableRef table, ResultSet rows, List<OracleColumn> columns, ForEachRow forEach) throws SQLException {
        Map<String, Object> row = new HashMap<>();

        // queryFirst column needs to be read first before all other columns.
        // Otherwise it throws "Stream has already been closed" error
        // LONG is the only column type that exhibits this behavior
        columns.stream()
                .filter(c -> c.oracleType.isLong())
                .forEach(
                        c -> {
                            Object value = outputHelper.coerce(table, rows, c);
                            row.put(c.name, value);
                        });

        // now read the other columns

        StringBuilder errorMsg = null;
        for (OracleColumn c : columns) {
            if (c.oracleType.isLong()) continue;
            try {
                Object value = outputHelper.coerce(table, rows, c);
                row.put(c.name, value);
            } catch (Exception e) {
                if (NUMERIC_OVERFLOW.isCauseOf(e)) {
                    if (invalidNumberWarningCount < MAX_INVALID_NUMBER_WARNING_BEFORE_GIVING_UP) {
                        Object value = rows.getString(c.name);
                        if (errorMsg == null) {
                            errorMsg = new StringBuilder();
                            errorMsg.append("Error in Table: " + table + "\n");
                        }
                        errorMsg.append("Cannot read row " + c.name + " VALUE = " + value + ".\n");
                        errorMsg.append("The destination value will be set to NULL.\n");
                        errorMsg.append("Please confirm and correct the following value in the source table.\n");
                        errorMsg.append(
                                "SELECT "
                                        + c.name
                                        + " FROM "
                                        + RowUtils.doubleQuote(table)
                                        + " WHERE ROWID = "
                                        + RowUtils.singleQuote(rows.getString(ROW_ID_COLUMN_NAME)));
                        row.put(c.name, null);
                        invalidNumberWarningCount++;
                        continue;
                    } else {
                        LOG.customerInfo(
                                InfoEvent.of(
                                        "warning",
                                        "Giving up sync ... Too many invalid numbers. Please correct values before retry."));
                    }
                }
                throw e;
            }
        }
        if (errorMsg != null) {
            LOG.customerInfo(InfoEvent.of("warning", errorMsg.toString()));
        }

        //String rowId = metaDataUtil.asConstant(rows.getString(ROW_ID_COLUMN_NAME));
        String rowId = "1";

        forEach.accept(rowId, row);
    }

    public LogMinerSession newLogMinerSession(Connection connection) {
        return new LogMinerSession(connection);
    }

    /**
     * @TODO: Need to set business logic fpor getLogMinerStream
     * @param connection
     * @param includedTables
     * @param hasHistoryMode
     * @param fetchSizeTracker
     * @return
     */
    public PreparedStatement getLogMinerStream(Connection connection,
                                               Map<TableRef, List<OracleColumn>> includedTables,
                                               boolean hasHistoryMode,
                                               FetchSizeTracker fetchSizeTracker) {
        String query = "";
        try {
            return connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Need to write business logic for getNlsDateFormat
     * @param connection
     * @return
     */
    public PreparedStatement getNlsDateFormat(Connection connection) {
        String query = "";
        try {
            return connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Need to write business logic for getNlsTimestampFormat
     * @param connection
     * @return
     */
    public PreparedStatement getNlsTimestampFormat(Connection connection) {

        String query = "";
        try {
            return connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Need to write business logic for getNlsTimestampTzFormat
     * @param connection
     * @return
     */
    public PreparedStatement getNlsTimestampTzFormat(Connection connection) {
        String query = "";
        try {
            return connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @TODO: Need to write business logic
     * @param toString
     * @param table
     */
    public void logUncommittedTransactionWarning(String toString, Object table) {
    }
}