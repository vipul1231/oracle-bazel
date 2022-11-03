package com.example.snowflakecritic.ibf;

import com.example.ibf.StrataEstimatorUtils;
import com.example.ibf.db_compare.IbfCompareColumn;
import com.example.ibf.db_compare.IbfCompareTableDestination;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.google.common.collect.ImmutableMap;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SnowflakeIbfCompareDestinationAdapter extends SnowflakeIbfBase
        implements IbfCompareTableDestination {
    public SnowflakeIbfCompareDestinationAdapter(DataSource source, SnowflakeTableInfo tableInfo) {
        super(source, tableInfo);
    }

    @Override
    public InvertibleBloomFilter fetchCommonIbf(int cellCount, List<IbfCompareColumn> sourceColumns)
            throws SQLException {
        InvertibleBloomFilter ibf =
                new InvertibleBloomFilter(IbfDbUtils.computeKeyLengthsSum(keyLengths()), cellCount);
        String query =
                IbfDbUtils.generateCommonIbfQuery(
                        TEMPLATE_FILENAME, cellCount, true, templateParams(sourceColumns));

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            IbfDbUtils.loadInvertibleBloomFilterFromResultSet(ibf, rs);
        }
        return ibf;
    }


    @Override
    public List<IbfCompareIntermediateRow> fetchIntermediateRowsForTesting(
            List<IbfCompareColumn> sourceColumns) throws SQLException {
        int keyLengthsSum = keyLengths().stream().mapToInt(Integer::intValue).sum();
        List<IbfCompareIntermediateRow> rows = new ArrayList<>();

        String query =
                IbfDbUtils.generateIntermediateRowsTemplate(
                        TEMPLATE_FILENAME, true, templateParams(sourceColumns));

        try (Connection connection = source.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            IbfDbUtils.loadIntermediateRowsFromResultSet(keyLengthsSum, rows, rs);
        }

        return rows;
    }

    private Map<String, Object> templateParams(List<IbfCompareColumn> sourceColumns) {
        List<String> columnNames =
                tableInfo.sourceColumns().stream().map(c -> c.getName()).collect(Collectors.toList());

        return new ImmutableMap.Builder<java.lang.String, java.lang.Object>()
                .put("primaryKeys", getKeyNames())
                .put("columns", sourceColumns)
                .put("table", tableRef())
                .put("keyLengths", keyLengths())
                .put("keyTypes", keyTypes())
                .put("syncMode", StrataEstimatorUtils.syncModeFromColumnNames(columnNames))
                .put("helper", new SnowflakeIbfBase.TemplateHelper())
                .build();
    }
}