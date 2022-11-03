package com.example.ibf;

import java.util.Arrays;

public class IBFDecodeResultElement {
    public final long[] keySum;
    public final long rowHashSum;

    public IBFDecodeResultElement(long[] keySum, long rowHashSum) {
        this.keySum = keySum;
        this.rowHashSum = rowHashSum;
    }

    public long[] getKeySum() {
        return keySum;
    }

    public long getRowHashSum() {
        return rowHashSum;
    }

    @Override
    public String toString() {
        return "{keySum=" + Arrays.toString(keySum) + ", rowHashSum=" + rowHashSum + "}";
    }
}
