package com.example.ibf;

import com.example.core.annotations.DataType;
import java.util.Arrays;
import java.util.List;

public class StrataEstimator {

    private static final int STRATUM_COUNT = 64;
    public static final int CELLS_COUNT = 160;
    public static final double CORRECTION_OVERHEAD = 1;

    protected List<DataType> keyTypes;
    protected List<Integer> keyLengths;

    public InvertibleBloomFilter[] stratum = new SEInvertibleBloomFilter[STRATUM_COUNT];

    public StrataEstimator(List<DataType> keyTypes, List<Integer> keyLengths) {
        this.keyLengths = keyLengths;
        this.keyTypes = keyTypes;
        for (int i = 0; i < STRATUM_COUNT; i++) {
            stratum[i] = new SEInvertibleBloomFilter(keyTypes, keyLengths, CELLS_COUNT);
        }
    }

    public void loadFromDatabase(int strata, int cellIndex, long[] keySum, long keyHash, long count) {
        stratum[strata].getCell(cellIndex).load(keySum, keyHash, count);
    }

    public void insert(long[] key, long keyHash) {
        int partition = getNumberOfTrailingZeroes(keyHash);
        stratum[partition].insert(key, keyHash);
    }

    private int getNumberOfTrailingZeroes(long x) {
        return x == 0 ? 0 : Long.numberOfTrailingZeros(x);
    }

    public static StrataEstimatorDecodeResult estimateDifference(StrataEstimator seA, StrataEstimator seB) {
        StrataEstimatorDecodeResult result = new StrataEstimatorDecodeResult(seA.keyTypes, seB.keyLengths);
        result.count = 0;
        for (int i = 63; i >= 0; i--) {
            InvertibleBloomFilter diff = seA.stratum[i].subtract(seB.stratum[i]);
            IBFDecodeResult resultIBF = diff.decode();
            if (resultIBF.succeeded) {
                for (int j = 0; j < resultIBF.aWithoutB.size(); j++) {
                    result.aWithoutB.add(resultIBF.aWithoutB.get(j));
                }
                for (int j = 0; j < resultIBF.bWithoutA.size(); j++) {
                    result.bWithoutA.add(resultIBF.bWithoutA.get(j));
                }
                result.count += resultIBF.aWithoutB.size() + resultIBF.bWithoutA.size();
            } else {
                result.count = (int) Math.pow(2.0f, (float) (i + 1)) * (int) (CORRECTION_OVERHEAD * (result.count + 1));
                break;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return Arrays.toString(stratum);
    }

    public List<Integer> keyLengths() {
        return keyLengths;
    }
}
