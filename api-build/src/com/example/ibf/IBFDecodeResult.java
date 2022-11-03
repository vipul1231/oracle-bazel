package com.example.ibf;

public class IBFDecodeResult extends DecodeResult {
    /**
     * true indicates that the decode operation fully completed; false indicates some or all of the cells failed to
     * decode
     */
    public boolean succeeded;

    @Override
    public String toString() {
        return "IBFDecodeResult { succeeded="
                + succeeded
                + " aWithoutB="
                + listToStringWithTruncate(aWithoutB)
                + " bWithoutA="
                + listToStringWithTruncate(bWithoutA)
                + " }";
    }

    public long dataSize() {
        long size = 0L;
        if (!aWithoutB.isEmpty()) size += aWithoutB.size() * (8 * (aWithoutB.get(0).keySum.length + 1));
        if (!bWithoutA.isEmpty()) size += bWithoutA.size() * (8 * (bWithoutA.get(0).keySum.length + 1));
        return size;
    }
}
