package com.example.snowflakecritic.ibf;

import com.example.core.annotations.DataType;
import com.example.ibf.IbfTemplate;
import com.example.ibf.OneHashingBloomFilterUtils;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.ibf.db_incremental_sync.IbfTableEncoder;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IbfDbUtils {
    private static final int SINGLE_KEY_SIZE = 8;

    /**
     * We use this technique to calculate the nuymber of trailing zero bits:
     * https://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightModLookup The inner modulus division is
     * performed in the database and the lookup is performed in Java by the utility methods in this class. *
     */
    private static final int[] STRATA_ESTIMATOR_MOD_37_BIT_POSITION = // map a bit value mod 37 to its position
            new int[] {
                    32, 0, 1, 26, 2, 23, 27, 0, 3, 16, 24, 30, 28, 11, 0, 13, 4, 7, 17, 0, 25, 22, 31, 15, 29, 10, 12, 6, 0,
                    21, 14, 9, 5, 20, 8, 19, 18
            };

    private IbfDbUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void loadInvertibleBloomFilterFromResultSet(ResizableInvertibleBloomFilter ibf, ResultSet rs)
            throws SQLException {
        int keyLengthsSum = ibf.keyLengthsSum();
        while (rs.next()) {
            int columnIndex = 1;
            int cellIndex = rs.getInt(columnIndex++);
            long[] keySums = new long[keyLengthsSum];
            for (int i = 0; i < keyLengthsSum; i++) {
                keySums[i] = rs.getLong(columnIndex++);
            }
            long rowHashSum = rs.getLong(columnIndex++);
            long cCount = rs.getLong(columnIndex);
            ibf.loadFromDatabase(cellIndex, keySums, rowHashSum, cCount);
        }
    }

    public static void loadInvertibleBloomFilterFromResultSet(InvertibleBloomFilter ibf, ResultSet rs)
            throws SQLException {
        int keyLengthsSum = ibf.keyLengthsSum();
        while (rs.next()) {
            int columnIndex = 1;
            int cellIndex = rs.getInt(columnIndex++);
            long[] keySums = new long[keyLengthsSum];
            /*for (int i = 0; i < keyLengthsSum; i++) {
                keySums[i] = rs.getLong(columnIndex++);
            }*/
            long rowHashSum = rs.getLong(columnIndex++);
            long cCount = rs.getLong(columnIndex);
            ibf.loadFromDatabase(cellIndex, keySums, rowHashSum, cCount);
        }
    }


    public static void loadIntermediateRowsFromResultSet(
            int keyLengthsSum, List<IbfCompareIntermediateRow> rows, ResultSet rs) throws SQLException {
        while (rs.next()) {
            int columnIndex = 1;

            long[] keys = new long[keyLengthsSum];
            for (int i = 0; i < keyLengthsSum; i++) {
                keys[i] = rs.getLong(columnIndex++);
            }

            long rowHash = rs.getLong(columnIndex++);
            String md5Hash = rs.getString(columnIndex++);
            String rowRepresentation = rs.getString(columnIndex);

            rows.add(new IbfCompareIntermediateRow(keys, rowHash, md5Hash, rowRepresentation));
        }
    }

    public static void loadInvertibleBloomFilterFromResultSetWithInt(InvertibleBloomFilter ibf, ResultSet rs)
            throws SQLException {
        int keyLengthsSum = ibf.keyLengthsSum();
        while (rs.next()) {
            int columnIndex = 1;
            int cellIndex = rs.getInt(columnIndex++);
            long[] keySums = new long[keyLengthsSum];
            for (int i = 0; i < keyLengthsSum; i++) {
                keySums[i] = rs.getInt(columnIndex++);
            }
            int rowHashSumHi = rs.getInt(columnIndex++);
            int rowHashSumLo = rs.getInt(columnIndex++);
            long rowHashSum = concatToLong(rowHashSumHi, rowHashSumLo);

            long cCount = rs.getLong(columnIndex);
            ibf.loadFromDatabase(cellIndex, keySums, rowHashSum, cCount);
        }
    }

    public static void loadPrimaryKeyStrataEstimatorFromResultSet(StrataEstimator se, ResultSet rs)
            throws SQLException {
        int keySumsLength = computeKeyLengthsSum(se.keyLengths);
        while (rs.next()) {
            int columnIndex = 1;
            int strataIndexForLookup = rs.getInt(columnIndex++);
            int cellIndex = rs.getInt(columnIndex++);
            long[] keySums = new long[keySumsLength];
            for (int i = 0; i < keySumsLength; i++) {
                keySums[i] = rs.getLong(columnIndex++);
            }
            long keyHashSum = rs.getLong(columnIndex++);
            long cCount = rs.getLong(columnIndex);

            se.loadFromDatabase(
                    STRATA_ESTIMATOR_MOD_37_BIT_POSITION[strataIndexForLookup], cellIndex, keySums, keyHashSum, cCount);
        }
    }

    public static String generateInvertibleBloomFilterQuery(
            String templateFilename, int cellCount, Map<String, Object> templateParams) {
        long[] divisors =
                OneHashingBloomFilterUtils.primeDivisors(InvertibleBloomFilter.K_INDEPENDENT_HASH_FUNCTIONS, cellCount);

        return getInvertibleBloomFilterTemplate(templateFilename, divisors, templateParams);
    }

    public static String generateCommonIbfQuery(
            String templateFilename, int cellCount, boolean isDestination, Map<String, Object> templateParams) {
        long[] divisors =
                OneHashingBloomFilterUtils.primeDivisors(InvertibleBloomFilter.K_INDEPENDENT_HASH_FUNCTIONS, cellCount);

        return generateCommonIbfTemplate(templateFilename, divisors, isDestination, templateParams);
    }

    public static String generateResizableInvertibleBloomFilterQuery(
            String templateFilename,
            int smallCellCount,
            ResizableInvertibleBloomFilter.Sizes size,
            Map<String, Object> templateParams) {

        long[] divisors =
                OneHashingBloomFilterUtils.resizingDivisors(
                        InvertibleBloomFilter.K_INDEPENDENT_HASH_FUNCTIONS, smallCellCount, size.resizingFactors);

        return getInvertibleBloomFilterTemplate(templateFilename, divisors, templateParams);
    }

    public static String generatePrimaryKeyStrataEstimatorQuery(
            String templateFilename, Map<String, Object> configurationParams) {
        long[] divisors =
                OneHashingBloomFilterUtils.primeDivisors(
                        InvertibleBloomFilter.K_INDEPENDENT_HASH_FUNCTIONS, StrataEstimator.CELLS_COUNT);

        Map<String, Object> combinedParams =
                ImmutableMap.<String, Object>builder()
                        .putAll(configurationParams)
                        .put("cellsCount", OneHashingBloomFilterUtils.totalCellCount(divisors))
                        .put("primeDivisors", divisors)
                        .put("partitionOffsets", OneHashingBloomFilterUtils.partitionOffsets(divisors))
                        .build();

        return IbfTemplate.get(
                templateFilename, IbfTemplate.Output.PRIMARY_KEY_STRATA_ESTIMATOR, combinedParams);
    }

    private static String getInvertibleBloomFilterTemplate(
            String templateFilename, long[] divisors, Map<String, Object> templateParams) {
        Map<String, Object> combinedParams =
                ImmutableMap.<String, Object>builder()
                        .putAll(templateParams)
                        .put("cellsCount", OneHashingBloomFilterUtils.totalCellCount(divisors))
                        .put("primeDivisors", divisors)
                        .put("partitionOffsets", OneHashingBloomFilterUtils.partitionOffsets(divisors))
                        .build();

        return IbfTemplate.get(templateFilename, IbfTemplate.Output.INVERTIBLE_BLOOM_FILTER, combinedParams);
    }

    private static String generateCommonIbfTemplate(
            String templateFilename, long[] divisors, boolean isDestination, Map<String, Object> templateParams) {
        Map<String, Object> combinedParams =
                ImmutableMap.<String, Object>builder()
                        .putAll(templateParams)
                        .put("cellsCount", OneHashingBloomFilterUtils.totalCellCount(divisors))
                        .put("primeDivisors", divisors)
                        .put("partitionOffsets", OneHashingBloomFilterUtils.partitionOffsets(divisors))
                        .put("isDestination", isDestination)
                        .build();

        return IbfTemplate.get(
                templateFilename, IbfTemplate.Output.COMMON_INVERTIBLE_BLOOM_FILTER, combinedParams);
    }

    public static String generateIntermediateRowsTemplate(
            String templateFilename, boolean isDestination, Map<String, Object> templateParams) {
        Map<String, Object> combinedParams =
                ImmutableMap.<String, Object>builder()
                        .putAll(templateParams)
                        .put("isDestination", isDestination)
                        .build();

        return IbfTemplate.get(templateFilename, IbfTemplate.Output.COMMON_INTERMEDIATE_ROWS, combinedParams);
    }

    /** compute the key length of a primary key based on its type parameter * */
    public static int computeKeyLength(long typeParameter) {
        return computeKeyLength(typeParameter, SINGLE_KEY_SIZE);
    }

    /** compute the key length of a primary key based on its type parameter and given byte size of key * */
    public static int computeKeyLength(long typeParameter, int keySize) {
        if (typeParameter <= keySize) {
            return IbfTableEncoder.DEFAULT_KEY_LENGTH;
        }
        int keyLength = (int) (typeParameter / keySize);
        if (typeParameter % keySize > 0) keyLength += 1;
        return keyLength;
    }

    public static int computeKeyLengthsSum(List<Integer> keyLengths) {
        return keyLengths.stream().mapToInt(Integer::intValue).sum();
    }

    public static List<Object> decodePk(List<DataType> keyTypes, List<Integer> keyLengths, long[] keySums) {
        List<Object> keys = new ArrayList<>();
        int offset = 0;
        for (int i = 0; i < keyTypes.size(); i++) {
            switch (keyTypes.get(i)) {
                case Int:
                case Short:
                    keys.add(keySums[offset++]);
                    break;
                case Long:
                    if (keyLengths.get(i) == 2)
                        keys.add(concatToLong((int) keySums[offset++], (int) keySums[offset++]));
                    else keys.add(keySums[offset++]);
                    break;
                case BigDecimal:
                    // TODO(codd): this check should only apply for MySQL when the key type is BIGINT UNSIGNED
                    //                    if (keySums[offset] <= 0)
                    //                        throw new RuntimeException(
                    //                                "Invalid key sum value for BigDecimal data type: " +
                    // keySums[offset]);
                    keys.add(keySums[offset]);
                    offset++;
                    break;
                case String:
                case Binary:
                    keys.add(decodeStringPk(keySums, offset, keyLengths.get(i)));
                    offset += keyLengths.get(i);
                    break;
                default:
                    throw new RuntimeException("Unsupported primary key type: " + keyTypes.get(i));
            }
        }
        return keys;
    }

    private static String decodeStringPk(long[] keySum, int offset, int keyLength) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < keyLength; i++) {
            long key = keySum[offset + i];
            if (key == 0) continue;
            try {
                byte[] decodeHex = Hex.decodeHex(Long.toHexString(key).toCharArray());
                result.append(new String(decodeHex));
            } catch (DecoderException e) {
                throw new RuntimeException(e);
            }
        }
        return result.toString();
    }

    private static long concatToLong(int hi, int lo) {
        return ((long) hi << 32) | (0xFFFFFFFFL & lo);
    }
}
