package com.example.oracle.cache;

import com.example.core.TableRef;
import com.example.flag.FeatureFlag;
import com.example.logger.ExampleLogger;
import com.example.oracle.ConnectionFactory;
import com.example.oracle.Transactions;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.example.oracle.Constants.DEFAULT_FETCH_SIZE;
import static com.example.oracle.Constants.QUERY_LIST_MAX_SIZE;
import static com.example.oracle.Util.singleQuote;

/**
 * This class will hold the set of partitioned tables. This should be a helper method
 * We have to decide on when to initialize this cache
 * <p>
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/12/2021<br/>
 * Time: 7:58 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class PartitionedTableCache {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private static Set<TableRef> partitionedTablesWithRowMovementEnabled = new HashSet<>();
    private static Set<TableRef> partitionedTables = new HashSet<>();


    /**
     * you have to initialize the cache inorder to use it
     */
    public void initPartitionedTables(Set<TableRef> selected) {
        initPartitionedTables(new ArrayList<>(selected), QUERY_LIST_MAX_SIZE);
    }

    public void initPartitionedTables(List<TableRef> selected, int maxSize) {
        List<List<TableRef>> pages = Lists.partition(selected, maxSize);
        pages.forEach(this::initPartitionedTablePages);
    }

    public void initPartitionedTablePages(List<TableRef> selected) {
        LOG.info("Initialize partitioned table pages");

//        @Language("SQL")
        String query =
                "SELECT OWNER as TABLE_SCHEMA, TABLE_NAME , ROW_MOVEMENT FROM all_tables "
                        + "WHERE PARTITIONED = 'YES' AND "
                        + "OWNER IN ("
//                        + Joiner.on(",")
//                        .join(selected.stream().map(t -> singleQuote(t.schema)).collect(Collectors.toSet()))
                        + ") AND TABLE_NAME IN ("
                        + selected.stream().map(t -> singleQuote(t.name)).collect(Collectors.joining(","))
                        + ")"
                        + " AND TEMPORARY = 'N'";

        if (!FeatureFlag.check("OracleIncludeIotTables")) query += " AND IOT_TYPE IS NULL";

        String finalQuery = query;
        Transactions.RetryFunction<Set<TableRef>> action =
                (connection) -> {
                    try (Statement statement = connection.createStatement();
                         ResultSet results = statement.executeQuery(finalQuery)) {
                        results.setFetchSize(DEFAULT_FETCH_SIZE);
                        while (results.next()) {
                            String schema = results.getString("TABLE_SCHEMA");
                            String table = results.getString("TABLE_NAME");
                            partitionedTables.add(new TableRef(schema, table));

                            if ("ENABLED".equalsIgnoreCase(results.getString("ROW_MOVEMENT")))
                                partitionedTablesWithRowMovementEnabled.add(new TableRef(schema, table));
                        }
                    }
                    return null;
                };

        ConnectionFactory.getInstance().retry(
                "initPartitionedTables",
                t -> new RuntimeException("Error in getting list of partitioned tables", t),
                action);
    }

    /**
     * Names of all partition tables
     */
    public static Set<TableRef> getPartitionedTables() {
        return partitionedTables;
    }

    /**
     * Names of all partitioned tables with row movement enabled For any of these tables that are also pkeyless, we'll
     * use append-only strategy (hash of all columns as pkey)
     */
    public static Set<TableRef> partitionedTablesWithRowMovementEnabled() {
        return partitionedTablesWithRowMovementEnabled;
    }

}