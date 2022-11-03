package com.example.snowflakecritic.ibf;

import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.ibf.StrataEstimatorDecodeResult;
import com.example.ibf.db_incremental_sync.IbfTableEncoder;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
//import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;
import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;
import static com.example.snowflakecritic.ibf.SnowflakeIbfSpec.*;
import static com.example.sql_server.DbRowHelper.COMPOUND_PK_A;
import static com.example.sql_server.DbRowHelper.COMPOUND_PK_B;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnowflakeIbfAdapterSpec {
    @Test
    public void test_ResizableInvertibleBloomFilter_intPk() throws Exception {
        int minimumCellCount = 10;

        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(INT_PK_TABLE);

        ResizableInvertibleBloomFilter dbIBF =
                adapter.getResizableInvertibleBloomFilter(minimumCellCount, ResizableInvertibleBloomFilter.XLARGE);
        ResizableInvertibleBloomFilter testDataIBF =
                getResizableInvertibleBloomFilterFromRows(
                        minimumCellCount,
                        INT_PK_TABLE_DATA,
                        PRIMARY_KEY_NAME,
                        new String[] {PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME});

        IbfSyncResult result =
                new IbfSyncResult(
                        dbIBF.compare(testDataIBF),
                        ImmutableList.of(DataType.Int),
                        ImmutableList.of(IbfTableEncoder.DEFAULT_KEY_LENGTH));
        assertEquals(0, result.aWithoutB().size());
        assertEquals(0, result.bWithoutA().size());
    }

    @Test
    public void test_ResizableInvertibleBloomFilter_stringPk() throws Exception {
        int minimumCellCount = 10;

        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(STRING_PK_TABLE);

        ResizableInvertibleBloomFilter dbIBF =
                adapter.getResizableInvertibleBloomFilter(minimumCellCount, ResizableInvertibleBloomFilter.XLARGE);

        IbfSyncResult syncResult =
                new IbfSyncResult(dbIBF.decode(), adapter.keyTypes(), adapter.keyLengths());

        assertEquals(STRING_PK_TABLE_DATA.size(), syncResult.upserts().size());
        assertEquals(0, syncResult.deletes().size());
        for (Map<String, Object> row : STRING_PK_TABLE_DATA) {
            assertTrue(syncResult.upserts().contains(ImmutableList.of(row.get(PRIMARY_KEY_NAME))));
        }
    }

    @Test
    public void test_ResizableInvertibleBloomFilter_compoundPk() throws Exception {
        int minimumCellCount = 10;

        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(COMPOUND_PK_TABLE);

        ResizableInvertibleBloomFilter dbIBF =
                adapter.getResizableInvertibleBloomFilter(minimumCellCount, ResizableInvertibleBloomFilter.XLARGE);

        IbfSyncResult syncResult =
                new IbfSyncResult(dbIBF.decode(), adapter.keyTypes(), adapter.keyLengths());

        assertEquals(COMPOUND_PK_TABLE_DATA.size(), syncResult.upserts().size());
        assertEquals(0, syncResult.deletes().size());
        for (Map<String, Object> row : COMPOUND_PK_TABLE_DATA) {
            List<List<Object>> upserts = syncResult.upserts();
            Object o = row.get(COMPOUND_PK_A);
            Object o2 = row.get(COMPOUND_PK_B);
            List<Object> objects = syncResult.upserts().stream().findFirst().get();
            int i = 0;
            assertTrue(
                    syncResult
                            .upserts()
                            .contains(
                                    ImmutableList.of(
                                            Long.valueOf((int) row.get(COMPOUND_PK_A)), row.get(COMPOUND_PK_B))));
        }
    }

    @Test
    public void test_InvertibleBloomFilter() throws Exception {
        int cellsCount = 50;

        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(INT_PK_TABLE);

        InvertibleBloomFilter dbIBF = adapter.getInvertibleBloomFilter(cellsCount);
        InvertibleBloomFilter testDataIBF =
                getInvertibleBloomFilterFromRows(
                        cellsCount,
                        INT_PK_TABLE_DATA,
                        PRIMARY_KEY_NAME,
                        new String[] {PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME});

        IbfSyncResult result =
                new IbfSyncResult(
                        dbIBF.compare(testDataIBF),
                        ImmutableList.of(DataType.Int),
                        ImmutableList.of(IbfTableEncoder.DEFAULT_KEY_LENGTH));

        assertEquals(0, result.aWithoutB().size());
        assertEquals(0, result.bWithoutA().size());
    }

    @Test
    public void test_PrimaryKeyStrataEstimator() throws Exception {
        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(INT_PK_TABLE);

        StrataEstimator dbSE = adapter.getPrimaryKeyStrataEstimator();
        StrataEstimator testDataSE = getPrimaryKeyStrataEstimatorFromRows(INT_PK_TABLE_DATA, PRIMARY_KEY_NAME);

        StrataEstimatorDecodeResult result = StrataEstimator.estimateDifference(dbSE, testDataSE);
        assertEquals(0, result.count);
    }

    @Test
    public void test_retrieveRows() throws SQLException {
        SnowflakeIbfAdapter adapter = buildSnowflakeIbfAdapter(INT_PK_TABLE);

        Set<List<Object>> primaryKeyValues = ImmutableSet.of(ImmutableList.of(2), ImmutableList.of(7));
        Set<String> columns = ImmutableSet.of(PRIMARY_KEY_NAME, UPDATABLE_COLUMN_NAME);

        Map<String, Map<String, Object>> result = adapter.retrieveRows(primaryKeyValues, columns);

        assertEquals(2, result.size());
        assertEquals("bar2", result.get("2").get(UPDATABLE_COLUMN_NAME));
        assertEquals("bar7", result.get("7").get(UPDATABLE_COLUMN_NAME));
    }

    protected ResizableInvertibleBloomFilter getResizableInvertibleBloomFilterFromRows(
            int minimumCellCount, List<Map<String, Object>> rows, String primaryKeyColumnName, String[] checksumColumns)
            throws Exception {
        ResizableInvertibleBloomFilter testDataIBF =
                new ResizableInvertibleBloomFilter(IbfTableEncoder.DEFAULT_KEY_LENGTH, minimumCellCount);

        InvertibleBloomFilter invertibleBloomFilter =
                new InvertibleBloomFilter(IbfTableEncoder.DEFAULT_KEY_LENGTH, minimumCellCount);

        InvertibleBloomFilter returnInvertibleBloomFilter = insertRowsIntoIBF(invertibleBloomFilter, rows, primaryKeyColumnName, checksumColumns);

        ResizableInvertibleBloomFilter returnResizableInvertibleBloomFilter = new ResizableInvertibleBloomFilter(returnInvertibleBloomFilter.keyLengthsSum, minimumCellCount);

        return returnResizableInvertibleBloomFilter;

    }

    protected InvertibleBloomFilter getInvertibleBloomFilterFromRows(
            int cellsCount, List<Map<String, Object>> rows, String primaryKeyColumnName, String[] checksumColumns)
            throws Exception {
        InvertibleBloomFilter testDataIBF =
                new InvertibleBloomFilter(IbfTableEncoder.DEFAULT_KEY_LENGTH, cellsCount);

        return insertRowsIntoIBF(testDataIBF, rows, primaryKeyColumnName, checksumColumns);
    }

    protected StrataEstimator getPrimaryKeyStrataEstimatorFromRows(
            List<Map<String, Object>> rows, String primaryKeyName) {
        StrataEstimator testDataSE =
                new StrataEstimator(
                        ImmutableList.of(DataType.Long), ImmutableList.of(IbfTableEncoder.DEFAULT_KEY_LENGTH));
        addRowsToPrimaryKeySE(rows, testDataSE, primaryKeyName);

        return testDataSE;
    }

    private InvertibleBloomFilter insertRowsIntoIBF(
            InvertibleBloomFilter testDataIBF,
            List<Map<String, Object>> rows,
            String primaryKeyColumnName,
            String[] checksumColumns)
            throws Exception {
        for (Map<String, Object> row : rows) {
            String[] rowValues =
                    Arrays.stream(checksumColumns)
                            .map(c -> row.get(c).toString())
                            .collect(Collectors.toList())
                            .toArray(new String[0]);
            long checksum = snowflakeHash(rowValues);
            testDataIBF.insert(new long[] {Long.parseLong(row.get(primaryKeyColumnName).toString())}, checksum);
        }

        return testDataIBF;
    }

    private void addRowsToPrimaryKeySE(
            List<Map<String, Object>> rows, StrataEstimator testDataSE, String primaryKeyColumnName) {
        for (Map<String, Object> row : rows) {
            long checksum = md5_52bit_checksum(row.get(primaryKeyColumnName).toString());
            testDataSE.insert(new long[] {Long.parseLong(row.get(primaryKeyColumnName).toString())}, checksum);
        }
    }

    protected long md5_52bit_checksum(String input) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(input.getBytes());
        //String hexSubStr = Hex.encodeHexString(md.digest()).substring(0, 13);
        //return Long.parseLong(hexSubStr, 16);
        return Long.parseLong("0");
    }

    private SnowflakeIbfAdapter buildSnowflakeIbfAdapter(TableRef table) throws SQLException {
        SnowflakeTableInfo tableInfo = informer.tableInfo(table);

        assert SnowflakeIbfAdapter.canIbftSync(tableInfo);

        SnowflakeIbfAdapter adapter = new SnowflakeIbfAdapter(dataSource, tableInfo);

        return adapter;
    }

    /**
     * For incremental sync, we use the HASH utility function provided by Snowflake [1]. The internals of this function
     * are not public so we must manually calculate any values we need. The results of this calculation are stored in
     * the `precomputedHashes` map.
     *
     * <p>[1] https://docs.snowflake.com/en/sql-reference/functions/hash.html
     */
    protected long snowflakeHash(String[] row) {
        String values = String.join("|", row);

        Map<String, Long> precomputedHashses =
                new ImmutableMap.Builder<String, Long>()
                        .put("1|foo", -1262011083374036498L)
                        .put("2|bar2", -6324302018037326760L)
                        .put("3|bar3", 4649689755561375245L)
                        .put("4|bar4", -5369298585049911799L)
                        .put("5|bar5", 7845509883855656741L)
                        .put("6|bar6", -6307711818858791047L)
                        .put("7|bar7", 1657249235849945074L)
                        .put("8|bar8", -5344081176503086026L)
                        .put("9|bar9", 5041176239975046464L)
                        .put("10|bar10", 7651929230298818098L)
                        .put("11|bar11", 8232392334474817425L)
                        .build();

        if (!precomputedHashses.containsKey(values))
            throw new RuntimeException("snowflakeHash value missing for: " + values);

        return precomputedHashses.get(values);
    }
}
