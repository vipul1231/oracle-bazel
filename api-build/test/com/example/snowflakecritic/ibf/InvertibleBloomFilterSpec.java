package com.example.snowflakecritic.ibf;

import com.example.core.annotations.DataType;
import com.example.ibf.IBFDecodeResult;
import com.example.ibf.IbfSyncResult;
//import com.example.logger.ExampleLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.*;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class InvertibleBloomFilterSpec {
    //private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    @Test
    public void testIBFComputeDiff() {
        // arrange: Setup two IBFs that have 50 elements that differ
        int deleteCount = 50;
        float alpha = 2.0f;
        int size = (int) ((float) deleteCount * alpha);
        List<Integer> deletes = new ArrayList<>();

        Random rand = new Random(1);
        while (deletes.size() < deleteCount) {
            int delete = rand.nextInt(1000) - 500;
            if (deletes.contains(delete)) {
                continue;
            }

            deletes.add(delete);
        }

        InvertibleBloomFilter sourceIBF = new InvertibleBloomFilter(2, size);
        InvertibleBloomFilter destinationIBF = new InvertibleBloomFilter(2, size);

        for (int key = -500; key < 500; key++) {
            long simulatedHash = rand.nextLong();

            if (!deletes.contains(key)) {
                sourceIBF.insert(new long[] {key}, simulatedHash);
            }
            destinationIBF.insert(new long[] {key}, simulatedHash);
        }

        // act: Subtract and decode the two IBFs
        IBFDecodeResult decodeResult = sourceIBF.compare(destinationIBF);

        // assert
        assertTrue(decodeResult.succeeded);
        assertEquals(decodeResult.aWithoutB.size(), 0);
        assertEquals(decodeResult.bWithoutA.size(), deleteCount);
    }

    @Test
    public void testSubtractThrowsExceptionOnCellCountMismatch() {
        InvertibleBloomFilter ibf1 = new InvertibleBloomFilter(1, 10);
        InvertibleBloomFilter ibf2 = new InvertibleBloomFilter(1, 20);

        assertThrows(IllegalArgumentException.class, () -> ibf1.subtract(ibf2));
        assertThrows(IllegalArgumentException.class, () -> ibf2.subtract(ibf1));
    }

    @Test
    public void testSubtractThrowsExceptionOnKeyLengthsSumMismatch() {
        InvertibleBloomFilter ibf1 = new InvertibleBloomFilter(2, 10);
        InvertibleBloomFilter ibf2 = new InvertibleBloomFilter(1, 10);

        assertThrows(IllegalArgumentException.class, () -> ibf1.subtract(ibf2));
        assertThrows(IllegalArgumentException.class, () -> ibf2.subtract(ibf1));
    }

    @Test
    public void testIBF_keyLength1() {
        InvertibleBloomFilter ibf = new InvertibleBloomFilter(1, 3);
        long[] element = new long[] {5};

        ibf.insert(element, 0L);

        IBFDecodeResult result = ibf.decode();
        assert (result.succeeded);
        assertEquals(element[0], result.aWithoutB.get(0).keySum[0]);
    }

    @Test
    public void testIBF_keyLength2() {
        InvertibleBloomFilter ibf = new InvertibleBloomFilter(2, 3);
        long[] element = new long[] {5, 7};

        ibf.insert(element, 0L);

        IBFDecodeResult result = ibf.decode();
        assert (result.succeeded);
        assertEquals(element[0], result.aWithoutB.get(0).keySum[0]);
        assertEquals(element[1], result.aWithoutB.get(0).keySum[1]);
        assertEquals(0L, result.aWithoutB.get(0).rowHashSum);
    }

    @Test
    public void testIBF_keyLength3() {
        InvertibleBloomFilter ibf = new InvertibleBloomFilter(3, 3);
        long[] element = new long[] {5, 7, 11};

        ibf.insert(element, 0L);

        IBFDecodeResult result = ibf.decode();
        assert (result.succeeded);
        assertEquals(element[0], result.aWithoutB.get(0).keySum[0]);
        assertEquals(element[1], result.aWithoutB.get(0).keySum[1]);
        assertEquals(element[2], result.aWithoutB.get(0).keySum[2]);
        assertEquals(0L, result.aWithoutB.get(0).rowHashSum);
    }

    @Test
    public void testIBF_keyLength42() {
        final int ELEMENT_LENGTH = 42;

        InvertibleBloomFilter ibf = new InvertibleBloomFilter(ELEMENT_LENGTH, 3);
        long[] element = LongStream.range(0, ELEMENT_LENGTH).toArray();

        ibf.insert(element, 0L);

        IBFDecodeResult result = ibf.decode();
        assert (result.succeeded);
        for (int i = 0; i < ELEMENT_LENGTH; i++) assertEquals(element[i], result.aWithoutB.get(0).keySum[i]);
    }

    @Test
    public void testIBF_SerializerEncodeDecode() {
        InvertibleBloomFilter original = new InvertibleBloomFilter(2, 15);
        long[] element1 = new long[] {5, 7};
        long[] element2 = new long[] {6, 8};
        original.insert(element1, 10L);
        original.insert(element2, 11L);

        InvertibleBloomFilter.Serializer ibfSerializer = new InvertibleBloomFilter.Serializer();
        //byte[] ibfSerialized = ibfSerializer.encode(original);
        //InvertibleBloomFilter decoded = ibfSerializer.decode(ibfSerialized);

        //assertEquals(original.toString(), decoded.toString());
    }

    @Test
    public void testIBF_DecodeKeys() {
        InvertibleBloomFilter originalIbf = new InvertibleBloomFilter(5, 15);
        long[] keySums1 =
                new long[] {
                        3487250868110255417L, 3256163421987352881L, 7305732920317141293L, 3487256597543478323L, 1717985890L
                };
        originalIbf.insert(keySums1, 10L);

        long[] keySums2 =
                new long[] {
                        3487250868140716386L, 3256163421987352881L, 7305732920317141293L, 3487256597543478323L, 1717985890L
                };
        InvertibleBloomFilter newIbf = new InvertibleBloomFilter(5, 15);
        newIbf.insert(keySums2, 11L);

        IbfSyncResult result =
                new IbfSyncResult(
                        originalIbf.compare(newIbf), ImmutableList.of(DataType.String), ImmutableList.of(5));

        assertEquals(result.aWithoutB().size(), 1);
        assertEquals(result.bWithoutA().size(), 1);
        assertEquals(result.upserts(), ImmutableList.of(ImmutableList.of("0e404be9-07ab-11ec-b3aa-0e9f17d3ffbb")));
        assertEquals(result.deletes(), ImmutableSet.of(ImmutableList.of("0e40631b-07ab-11ec-b3aa-0e9f17d3ffbb")));
    }

    @Test
    public void testIBF_WithTwoKeys() {
        int keyLengthsSum = 6;
        InvertibleBloomFilter originalIbf = new InvertibleBloomFilter(keyLengthsSum, 15);
        long[] keySums1 =
                new long[] {
                        3487250868110255417L,
                        3256163421987352881L,
                        7305732920317141293L,
                        3487256597543478323L,
                        1717985890L,
                        1L
                };
        originalIbf.insert(keySums1, 10L);

        long[] keySums2 =
                new long[] {
                        3487250868140716386L,
                        3256163421987352881L,
                        7305732920317141293L,
                        3487256597543478323L,
                        1717985890L,
                        2L
                };
        InvertibleBloomFilter newIbf = new InvertibleBloomFilter(keyLengthsSum, 15);
        newIbf.insert(keySums2, 11L);

        IbfSyncResult result =
                new IbfSyncResult(
                        originalIbf.compare(newIbf),
                        ImmutableList.of(DataType.String, DataType.Int),
                        ImmutableList.of(5, 1));

        assertEquals(result.aWithoutB().size(), 1);
        assertEquals(result.bWithoutA().size(), 1);
        assertEquals(result.upserts(), ImmutableList.of((Arrays.asList("0e404be9-07ab-11ec-b3aa-0e9f17d3ffbb", 1L))));
        assertEquals(
                result.deletes(), Collections.singleton(Arrays.asList("0e40631b-07ab-11ec-b3aa-0e9f17d3ffbb", 2L)));
    }
}
