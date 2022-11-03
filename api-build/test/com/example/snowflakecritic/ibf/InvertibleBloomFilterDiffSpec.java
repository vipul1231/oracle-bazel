package com.example.snowflakecritic.ibf;

import com.example.ibf.IBFDecodeResult;
import com.example.ibf.IBFDecodeResultElement;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class InvertibleBloomFilterDiffSpec {
    final Map<Long, long[]> recordsHashLookupTable = new HashMap<>();

    @Test
    public void testInsertOneNewRecord() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(4l, ha(0l, 7l, 11l));

        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        addRecord(4, newIBF);
        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        assertThat(result.aWithoutB.size(), equalTo(1));
        assertThat(result.bWithoutA.size(), equalTo(0));
        assertThat(result.aWithoutB, contains(hasProperty("rowHashSum", equalTo(4l))));
    }

    @Test
    public void testInsertTwoNewRecord() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(3l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(4l, ha(0l, 7l, 11l));

        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        addRecord(3, newIBF);
        addRecord(4, newIBF);
        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        assertThat(result.aWithoutB.size(), equalTo(2));
        assertThat(result.bWithoutA.size(), equalTo(0));
        assertThat(result.aWithoutB, contains(hasProperty("rowHashSum", equalTo(4l)), hasProperty("rowHashSum", equalTo(3l))));
    }

    @Test
    public void testInsertTwoNewRecordWithSameHashes() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(3l, ha(3l, 7l, 11l));
        recordsHashLookupTable.put(4l, ha(3l, 7l, 11l));

        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        addRecord(3, newIBF);
        addRecord(4, newIBF);
        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        Assert.assertFalse(result.succeeded);
    }

    @Test
    public void testDeleteOneExistingRecord() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));

        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        removeRecord(2, newIBF);
        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        assertThat(result.aWithoutB.size(), equalTo(0));
        assertThat(result.bWithoutA.size(), equalTo(1));
        assertThat(result.bWithoutA, contains(hasProperty("rowHashSum", equalTo(2l))));
    }

    @Test
    public void testInsertAndDeleteRecord() {

    }

    @Test
    public void testInsertAndDeleteRecordWithSameHash() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(3l, ha(1l, 7l, 11l));


        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        addRecord(3, newIBF);
        removeRecord(2, newIBF);
        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        Assert.assertFalse(result.succeeded);
    }

    @Test
    public void testInsertTwoRecordsAndDeleteOneRecord() {
        //TODO to verify the 'clean up' logic
    }

    @Test
    public void testInsertSixRecords() {
        System.out.println("");

        recordsHashLookupTable.clear();
        recordsHashLookupTable.put(1l, ha(2l, 7l, 11l));
        recordsHashLookupTable.put(2l, ha(1l, 7l, 11l));
        recordsHashLookupTable.put(3l, ha(1l, 6l, 12l));
        recordsHashLookupTable.put(4l, ha(1l, 5l, 11l));
        recordsHashLookupTable.put(5l, ha(1l, 6l, 11l));
        recordsHashLookupTable.put(6l, ha(0l, 5l, 12l));
        recordsHashLookupTable.put(7l, ha(0l, 6l, 12l));
        recordsHashLookupTable.put(8l, ha(0l, 5l, 11l));

        InvertibleBloomFilter oldIBF = ibf();
        addRecord(1, oldIBF);
        addRecord(2, oldIBF);
        System.out.println("");
        print(oldIBF, "Old IBF");
        InvertibleBloomFilter newIBF = copy(oldIBF);
        addRecord(3, newIBF);
        addRecord(4, newIBF);
        addRecord(5, newIBF);
        addRecord(6, newIBF);
        addRecord(7, newIBF);
        addRecord(8, newIBF);

        System.out.println("");
        print(newIBF, "New IBF");
        InvertibleBloomFilter diff = diff(newIBF, oldIBF);
        print(diff, "Diff IBF");
        IBFDecodeResult result = diff.decode();
        print(result);

        Assert.assertFalse(result.succeeded);
    }

    private long[] ha(long... hashes) {
        return hashes;
    }

    private String str(long[] ls) {
        return format("[%s]", StringUtils.join(ls, ','));
    }

    private void print(IBFDecodeResult result) {
        System.out.println(result.succeeded ? "Diff succeeded !" : "Diff failed !");
        System.out.println(format("Inserted records: [%s]", toString(result.aWithoutB)));
        System.out.println(format("Deleted  records: [%s]", toString(result.bWithoutA)));
    }

    private String toString(List<IBFDecodeResultElement> elements) {
        return elements.stream().map( e -> String.valueOf(e.rowHashSum)).collect(Collectors.joining(","));
    }

    private void print(InvertibleBloomFilter ibf, String name) {
        System.out.println(format("****** %s ******\n%s\n", name,  ibf.toPrettyString()));
    }

    private InvertibleBloomFilter ibf() {
        InvertibleBloomFilter _ibf = new InvertibleBloomFilter(1, 7);
        useMockHashFunctions(_ibf);
        return _ibf;
    }

    private InvertibleBloomFilter copy(InvertibleBloomFilter ibf) {
        InvertibleBloomFilter _copy = ibf.copy();
        useMockHashFunctions(_copy);
        return _copy;
    }

    private InvertibleBloomFilter diff(InvertibleBloomFilter one, InvertibleBloomFilter another) {
        InvertibleBloomFilter _diff = one.subtract(another);
        useMockHashFunctions(_diff);
        return _diff;
    }

    private void useMockHashFunctions(InvertibleBloomFilter ibf) {
        try {
            FieldUtils.writeDeclaredField(ibf, "indicesHash",new Function<Long, long[]> () {
                @Override
                public long[] apply(Long id) {
                    return recordsHashLookupTable.get(id);
                }
            }, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void addRecord(long id, InvertibleBloomFilter ibf) {
        System.out.println(format("+ %d -> %s", id, str(recordsHashLookupTable.get(id))));
        ibf.insert(new long[]{ id }, id);
    }

    private void removeRecord(long id, InvertibleBloomFilter ibf) {
        System.out.println(format("- %d -> %s", id, str(recordsHashLookupTable.get(id))));
        ibf.remove(new long[]{ id }, id);
    }
}
