package com.example.snowflakecritic.ibf;



import com.example.core.Column;
import com.example.core.annotations.DataType;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.ibf.StrataEstimatorUtils;
import com.example.ibf.db_data_validator.IbfPrimaryKeyEncoder;
import com.example.ibf.db_incremental_sync.IbfTableEncoder;
import com.example.ibf.db_incremental_sync.IbfTableEncoderWithCompoundPK;
import com.example.snowflakecritic.SnowflakeSQLUtils;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class SnowflakeIbfAdapter extends SnowflakeIbfBase
        implements IbfTableEncoder, IbfTableEncoderWithCompoundPK, IbfPrimaryKeyEncoder {

    public static final List<DataType> ALLOWED_PRIMARY_KEY_TYPES =
            new ImmutableList.Builder<DataType>()
                    .add(DataType.Short)
                    .add(DataType.Int)
                    .add(DataType.Long)
                    .add(DataType.BigDecimal)
                    .add(DataType.String)
                    .build();

    public SnowflakeIbfAdapter(DataSource source, SnowflakeTableInfo tableInfo) {
        super(source, tableInfo);
    }

    public static boolean canIbftSync(SnowflakeTableInfo tableInfo) {
        return ibfTableExcludedReasons(tableInfo).isEmpty();
    }

    public static Set<String> ibfTableExcludedReasons(SnowflakeTableInfo tableInfo) {
        Set<String> reasons = new HashSet<>();

        List<Column> primaryKeyColumns =
                tableInfo.sourceColumns().stream().filter(c -> c.primaryKey).collect(Collectors.toList());

        if (primaryKeyColumns.size() == 0) {
            reasons.add("Table does not have a primary key");

            return reasons;
        }

        for (Column primaryKeyColumn : primaryKeyColumns) {
            if (!ALLOWED_PRIMARY_KEY_TYPES.contains(primaryKeyColumn.type))
                reasons.add("unsupported primary key type: " + primaryKeyColumn.type);

            if (primaryKeyColumn.type == DataType.BigDecimal) {
                if (primaryKeyColumn.scale.orElse(-1) != 0)
                    reasons.add("primary key column of type BigDecimal must have Scale = 0: " + primaryKeyColumn.name);
            }
        }

        return reasons;
    }

    @Override
    public boolean ifReplacementRequired() {
        return false;
    }

    @Override
    public ResizableInvertibleBloomFilter getResizableInvertibleBloomFilter(
            int minimumCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException {
        ResizableInvertibleBloomFilter ibf =
                new ResizableInvertibleBloomFilter(
                        IbfDbUtils.computeKeyLengthsSum(keyLengths()), minimumCellCount, size);

        String query =
                IbfDbUtils.generateResizableInvertibleBloomFilterQuery(
                        TEMPLATE_FILENAME, minimumCellCount, size, configurationParams());

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            IbfDbUtils.loadInvertibleBloomFilterFromResultSet(ibf, rs);
        }
        return ibf;
    }

    @Override
    public ResizableInvertibleBloomFilter getReplacementResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException {
        throw new UnsupportedOperationException("getReplacementResizableInvertibleBloomFilter is not yet implemented");
    }

    @Override
    public InvertibleBloomFilter getInvertibleBloomFilter(int cellCount) throws SQLException {
        InvertibleBloomFilter ibf =
                new com.example.snowflakecritic.ibf.InvertibleBloomFilter(IbfDbUtils.computeKeyLengthsSum(keyLengths()), cellCount);
        String query =
                com.example.snowflakecritic.ibf.IbfDbUtils.generateInvertibleBloomFilterQuery(TEMPLATE_FILENAME, cellCount, configurationParams());

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            com.example.snowflakecritic.ibf.IbfDbUtils.loadInvertibleBloomFilterFromResultSet(ibf, rs);
        }
        return ibf;
    }

    @Override
    public int keyLength() {
        if (primaryKeyColumns.size() > 1) throw new IllegalStateException("There is more than one primary key");
        return keyLengths().get(0);
    }

    @Override
    public DataType keyType() {
        if (primaryKeyColumns.size() > 1) throw new IllegalStateException("There is more than one primary key");
        return keyTypes().get(0);
    }

    @Override
    public StrataEstimator getPrimaryKeyStrataEstimator() throws SQLException {
        StrataEstimator se = new StrataEstimator(keyTypes(), keyLengths());
        String query = IbfDbUtils.generatePrimaryKeyStrataEstimatorQuery(TEMPLATE_FILENAME, configurationParams());

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            IbfDbUtils.loadPrimaryKeyStrataEstimatorFromResultSet(se, rs);
        }
        return se;
    }

    @Override
    public Map<String, Map<String, Object>> retrieveRows(Set<List<Object>> primaryKeyValues, Set<String> columns)
            throws SQLException {
        Map<String, Map<String, Object>> results = new HashMap<>();

        String query =
                "SELECT "
                        + String.join(", ", columns)
                        + " FROM "
                        + SnowflakeSQLUtils.quote(tableRef())
                        + " WHERE "
                        + SnowflakeSQLUtils.whereIn(getKeyNames(), primaryKeyValues);

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String column : columns) {
                    // column names from Snowflake ResultSet are upper case
                    row.put(column.toUpperCase(), rs.getObject(column.toUpperCase()));
                }
                results.putIfAbsent(row.get(getKeyNames().get(0)).toString(), row);
            }
        }

        return results;
    }

    @Override
    public Set<String> getColumns() {
        return tableInfo.sourceColumns().stream().map(c -> c.name).collect(Collectors.toSet());
    }

    private Map<String, Object> configurationParams() {
        List<String> columnNames = tableInfo.sourceColumns().stream().map(c -> c.name).collect(Collectors.toList());

        return new ImmutableMap.Builder<java.lang.String, java.lang.Object>()
                .put("primaryKeys", getKeyNames())
                .put("columns", tableInfo.sourceColumns())
                .put("table", tableRef())
                .put("keyLengths", keyLengths())
                .put("keyTypes", keyTypes())
                .put("syncMode", StrataEstimatorUtils.syncModeFromColumnNames(columnNames))
                .put("helper", new SnowflakeIbfBase.TemplateHelper())
                .build();
    }
}
