package com.example.ibf;

public class DecodedIBFDebugger {
    public final int keyLengthsSum;
    public final int cellsCount;
    public final int nonZeroCellCount;
    public final long maxCellCount;

    public DecodedIBFDebugger(InvertibleBloomFilter diffIBF) {
        if (!diffIBF.decoded)
            throw new IllegalArgumentException("only IBFs that have been decoded can be used with this class");

        this.keyLengthsSum = diffIBF.keyLengthsSum;
        this.cellsCount = diffIBF.getCells().length;

        int nonZeroCount = 0;
        long maxCount = 0;
        for (int i = 0; i < cellsCount; i++) {
            if (diffIBF.getCell(i) == null) continue;

            if (!diffIBF.getCell(i).isZero()) nonZeroCount++;
            maxCount = Math.max(diffIBF.getCell(i).getCount(), maxCount);
        }
        this.nonZeroCellCount = nonZeroCount;
        this.maxCellCount = maxCount;
    }

    enum DecodeFailureReason {
        /**
         * The decode operation failed most probably because multiple elements in the diff IBF were inserted into the
         * same 3 cells. i.e., the 3 indepedent hash functions used to compute the indices each return the same result
         * when operating on multiple elements
         */
        INDICES_COLLISION,

        /** The number of differences between the compared IBFs exceeds the capacity of the IBF */
        TOO_MANY_DIFFERENCES,

        UNKNOWN
    }

    public DecodeFailureReason determineDecodeFailureReason() {
        if (hasNearlyAllNonZeroCells()) return DecodeFailureReason.TOO_MANY_DIFFERENCES;
        if (isProbableCollision()) return DecodeFailureReason.INDICES_COLLISION;

        return DecodeFailureReason.UNKNOWN;
    };

    private boolean hasNearlyAllNonZeroCells() {
        return nonZeroCellCount > (0.9 * cellsCount);
    }

    private boolean isProbableCollision() {
        /**
         * Only a small number (we chose 15 as an arbitrary threshold) of non-zero cells AND the number of non-zero is a
         * multiple of the number of independent hash functions used to determine cell indices in the IBF
         */
        return (nonZeroCellCount <= 15) && (nonZeroCellCount % InvertibleBloomFilter.K_INDEPENDENT_HASH_FUNCTIONS == 0);
    }

    @Override
    public String toString() {
        return "DecodedIBFDebugger{"
                + "keyLengthsSum="
                + keyLengthsSum
                + ", cellsCount="
                + cellsCount
                + ", nonZeroCellCount="
                + nonZeroCellCount
                + ", maxCellCount="
                + maxCellCount
                + '}';
    }
}
