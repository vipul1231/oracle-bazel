package com.example.oracle;

import com.example.db.SqlStatementUtils;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.ibf.db_incremental_sync.IbfTableEncoder;
import com.example.lambda.Lazy;
import com.example.snowflakecritic.ibf.IbfDbUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class OracleIBFQueryBuilder {
    private static final String TEMPLATE_FILENAME = "/oracle/resources/oracle_ibf.sql.vm";

    private static final String FT_BITXOR_IMPL_TYPE_SQL = "/oracle/resources/FT_BITXOR_IMPL_TYPE.sql";
    private static final String FT_BITXOR_IMPL_BODY_SQL = "/oracle/resources/FT_BITXOR_IMPL_BODY.sql";
    private static final String FT_BITXOR_FUNCTION_SQL = "/oracle/resources/FT_BITXOR_FUNCTION.sql";

    private static final String TEST_BITXOR_FUNCTION = "SELECT FT_BITXOR(0) FROM DUAL";

    private static final int RAW_KEY_SIZE = 4;

    public enum IBFType {
        REGULAR, // all columns
        REPLACEMENT, // all columns minus modified columns
        TRANSITION // all columns including modified columns w/default values
    }

    private IBFType ibfType = IBFType.REGULAR;

    private int cellCount;
    private boolean fixedSize;
    private ResizableInvertibleBloomFilter.Sizes ibfSizes;
    private OracleIbfTableInfo ibfTableInfo;

    //private Lazy<String> bitXorTypeExpression = new Lazy<>(this::loadCreateBitXorTypeStatement);
    //private Lazy<String> bitXorBodyExpression = new Lazy<>(this::loadCreateBitXorBodyStatement);
    //private Lazy<String> bitXorFunctionExpression = new Lazy<>(this::loadCreateBitXorFunctionStatement);

    private Lazy<String> bitXorTypeExpression;
    private Lazy<String> bitXorBodyExpression;
    private Lazy<String> bitXorFunctionExpression;



    private List<Integer> keyLengths;
    private int sumKeyLengths;
    private OracleColumnInfo[] arrayOfPrimaryKeys;

    public OracleIBFQueryBuilder(OracleIbfTableInfo ibfTableInfo) {
        if (ibfTableInfo.getOracleTableInfo().getPrimaryKeys().isEmpty()) {
            throw new RuntimeException(ibfTableInfo.getTableRef() + " does not have a primary key");
        }

        this.arrayOfPrimaryKeys = new OracleColumnInfo[ibfTableInfo.getOracleTableInfo().getPrimaryKeys().size()];

        int index = 0;
        for (OracleColumnInfo pkColInfo : ibfTableInfo.getOracleTableInfo().getPrimaryKeys()) {
            arrayOfPrimaryKeys[index++] = pkColInfo;
        }

        this.ibfTableInfo = ibfTableInfo;

        keyLengths =
                ibfTableInfo
                        .getOracleTableInfo()
                        .getPrimaryKeys()
                        .stream()
                        .map(colInfo -> OracleIBFQueryBuilder.numberOfColumnsForKey(colInfo))
                        .collect(Collectors.toList());

        sumKeyLengths = keyLengths.stream().reduce(0, Integer::sum).intValue();
    }

    public OracleIBFQueryBuilder setIbfType(IBFType ibfType) {
        this.ibfType = ibfType;
        return this;
    }

    public OracleIBFQueryBuilder setIbfSizes(ResizableInvertibleBloomFilter.Sizes ibfSizes) {
        this.ibfSizes = ibfSizes;
        return this;
    }

    public OracleIBFQueryBuilder setFixedSize(boolean val) {
        fixedSize = val;
        return this;
    }

    public OracleIBFQueryBuilder setCellCount(int cellCount) {
        this.cellCount = cellCount;
        return this;
    }

    public List<Integer> getKeyLengths() {
        return keyLengths;
    }

    public String getCreateBitXorTypeStatement() {
        return bitXorTypeExpression.get();
    }

    public String getTestBitXorFunction() {
        return TEST_BITXOR_FUNCTION;
    }

    private String loadCreateBitXorTypeStatement() throws IOException {
        return resourceToString(FT_BITXOR_IMPL_TYPE_SQL);
    }

    public String getCreateBitXorBodyStatement() {
        return bitXorBodyExpression.get();
    }

    private String loadCreateBitXorBodyStatement() throws IOException {
        return resourceToString(FT_BITXOR_IMPL_BODY_SQL);
    }

    public String getCreateBitXorFunctionStatement() {
        return bitXorFunctionExpression.get();
    }

    private String loadCreateBitXorFunctionStatement() throws IOException {
        return resourceToString(FT_BITXOR_FUNCTION_SQL);
    }

    private String resourceToString(String path) throws IOException {
        return null;
        //return IOUtils.toString(getClass().getResource(path));
    }

    public String buildQuery() {
        return doBuildQuery(templateParameters());
    }

    private String doBuildQuery(Map<String, Object> templateParameters) {
        return fixedSize
                ? IbfDbUtils.generateInvertibleBloomFilterQuery(TEMPLATE_FILENAME, cellCount, templateParameters)
                : IbfDbUtils.generateResizableInvertibleBloomFilterQuery(
                TEMPLATE_FILENAME,
                cellCount,
                Objects.requireNonNull(ibfSizes, "Call setIbfSizes(...) first"),
                templateParameters);
    }

    public class TemplateHelper {
        public boolean isBinary(OracleColumnInfo columnInfo) {
            return columnInfo.getType() == OracleType.Type.RAW;
        }

        public boolean isDateTime(OracleColumnInfo columnInfo) {
            return columnInfo.getOracleType().isDateTimeLike();
        }

        public boolean isNumber(OracleColumnInfo columnInfo) {
            return columnInfo.getOracleType().isNumber();
        }

        public boolean isString(OracleColumnInfo columnInfo) {
            return columnInfo.getOracleType().isCharacterString();
        }

        public boolean isUnicode(OracleColumnInfo columnInfo) {
            return columnInfo.getOracleType().isUnicodeString();
        }
    }

    @VisibleForTesting
    Map<String, Object> templateParameters() {
        List<OracleColumnInfo> columns = null;

        switch (ibfType) {
            case REGULAR:
            case TRANSITION:
                columns = ibfTableInfo.getOracleTableInfo().getIncomingColumns();
                break;
            case REPLACEMENT:
                columns = getColumnsWithoutModifiedColumns();
                break;
        }

        ImmutableMap.Builder<String, Object> builder =
                new ImmutableMap.Builder<String, Object>()
                        .put("primaryKeys", arrayOfPrimaryKeys)
                        .put("keyCount", arrayOfPrimaryKeys.length)
                        .put("columns", columns)
                        .put("table", SqlStatementUtils.ORACLE.quote(ibfTableInfo.getTableRef()))
                        .put("keyLength", sumKeyLengths)
                        .put("keyLengths", keyLengths)
                        .put("helper", new TemplateHelper());

        return builder.build();
    }

    public static int numberOfColumnsForKey(OracleColumnInfo keyColumn) {
        if (OracleType.Type.RAW.equals(keyColumn.getType())) {
            return computeKeyLengthBinary(keyColumn.getLength().orElse(IbfTableEncoder.DEFAULT_KEY_LENGTH));
        }
        return IbfDbUtils.computeKeyLength(keyColumn.getLength().orElse(IbfTableEncoder.DEFAULT_KEY_LENGTH));
    }

    public static int computeKeyLengthBinary(long typeParameter) {
        if (typeParameter <= RAW_KEY_SIZE) {
            return IbfTableEncoder.DEFAULT_KEY_LENGTH;
        }
        int keyLength = (int) (typeParameter / RAW_KEY_SIZE);
        if (typeParameter % RAW_KEY_SIZE > 0) keyLength += 1;
        return keyLength;
    }

    @VisibleForTesting
    List<OracleColumnInfo> getColumnsWithoutModifiedColumns() {
        return ibfTableInfo
                .getOracleTableInfo()
                .getIncomingColumns()
                .stream()
                .filter(columnInfo -> !columnInfo.isAddedSinceLastSync())
                .collect(Collectors.toList());
    }

    public int getSumKeyLengths() {
        return sumKeyLengths;
    }
}
