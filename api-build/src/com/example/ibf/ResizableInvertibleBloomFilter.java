package com.example.ibf;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * The ResizableInvertibleBloomFilter extends the InvertibleBloomFilter with the ability to change the number of cells.
 * For Ibf Incremental Syncs, we will persist a ResizableInvertibleBloomFilter in cloud storage and then, when we
 * perform a sync, we can start with a smaller sized IBF. If the smaller sized IBF fails to decode we can resize it to a
 * larger size and try again.
 *
 * <p>The ResizableInvertibleBloomFilter can be resized from the initial size to 3 smaller sizes: SMALL, MEDIUM or LARGE
 *
 * @see <a href="https://example.slab.com/posts/resizable-ibf-6bcc9tq4">Resizable IBF - Slab</a>
 */
public class ResizableInvertibleBloomFilter extends InvertibleBloomFilter {
    /**
     * SMALL has ~smallCellCount # of cells
     */
    public static final Sizes SMALL = Sizes.SMALL;
    /**
     * MEDIUM has ~5 * smallCellCount # of cells
     */
    public static final Sizes MEDIUM = Sizes.MEDIUM;
    /**
     * LARGE has ~17 * smallCellCount # of cells
     */
    public static final Sizes LARGE = Sizes.LARGE;
    /**
     * XLARGE has ~114 * smallCellCount # of cells
     */
    public static final Sizes XLARGE = Sizes.XLARGE;

    /**
     * Defines the order in which the sizes are attempted when syncing
     */
    public static final Sizes[] SIZES_ATTEMPT_ORDER = new Sizes[]{SMALL, MEDIUM, LARGE, XLARGE};

    @VisibleForTesting
    public final int smallCellCount;

    private final Sizes size;

    /**
     * Construct an empty Resizable Invertible Bloom Filter. The ResizableInvertibleBloomFilter will have smallCellCount
     * * ~114 # of cells.
     *
     * @param keyLengthsSum  the length of the long[] for elements' keys
     * @param smallCellCount the target number of minimum cells in the IBF (i.e., the SMALL IBF size)
     */
    public ResizableInvertibleBloomFilter(int keyLengthsSum, int smallCellCount) {
        this(keyLengthsSum, smallCellCount, XLARGE);
    }

    public Sizes getSize() {
        return size;
    }

    /**
     * Construct an empty Resizable Invertible Bloom Filter of a specific size.
     *
     * @param KeyLengthsSum  the length of the long[] for elements' keys
     * @param smallCellCount the target number of minimum cells in the IBF (i.e., the SMALL IBF size)
     * @param size           the size factor to scale this ResizableIBF by
     */
    public ResizableInvertibleBloomFilter(int KeyLengthsSum, int smallCellCount, Sizes size) {
        super(
                KeyLengthsSum,
                OneHashingBloomFilterUtils.resizingDivisors(
                        K_INDEPENDENT_HASH_FUNCTIONS, smallCellCount, size.resizingFactors));

        this.smallCellCount = smallCellCount;
        this.size = size;
    }

    ResizableInvertibleBloomFilter(ResizableInvertibleBloomFilter original, Sizes size) {
        super(original.keyLengthsSum, size.divisors(original.smallCellCount));

        this.smallCellCount = original.smallCellCount;
        this.size = size;
    }

    public ResizableInvertibleBloomFilter resize(Sizes newSize) {
        if (newSize == size) return this;
        if (!isResizingSupported(newSize))
            throw new IllegalStateException("It is not supported to resize this IBF instance to " + newSize);

        ResizableInvertibleBloomFilter resized = new ResizableInvertibleBloomFilter(this, newSize);
        long[] divisorPartitionSum = computeDivisorPartitionSum(divisors);
        long[] newDivisorPartitionSum = computeDivisorPartitionSum(resized.divisors);

        for (int i = 0; i < cells.length; i++) {
            Cell cell = this.getCell(i);
            if (!cell.isZero())
                resized.getCell(convertCellIndex(resized.divisors, divisorPartitionSum, newDivisorPartitionSum, i))
                        .add(cell);
        }

        return resized;
    }

    static int convertCellIndex(
            long[] newDivisors, long[] divisorPartitionSum, long[] newDivisorPartitionSum, int original) {
        int partitionIndex = 0;
        while (original - divisorPartitionSum[partitionIndex + 1] >= 0) partitionIndex++;

        long originalOffset = divisorPartitionSum[partitionIndex];
        long newOffset = newDivisorPartitionSum[partitionIndex];
        long divisor = newDivisors[partitionIndex];

        return Math.toIntExact(((original - originalOffset) % divisor) + newOffset);
    }

    private boolean isResizingSupported(Sizes newSize) {
        switch (size) {
            case XLARGE:
                return true;
            case LARGE:
            case MEDIUM:
                return newSize == Sizes.SMALL;
            default:
                throw new RuntimeException("Unsupported resizing size: " + size);
        }
    }

    static long[] computeDivisorPartitionSum(long[] divisors) {
        long[] divisorPartitionSum = new long[divisors.length + 1];
        for (int i = 0; i <= divisors.length; i++) divisorPartitionSum[i] = Arrays.stream(divisors).limit(i).sum();
        return divisorPartitionSum;
    }

    public static class Serializer extends ByteBufSerializer<ResizableInvertibleBloomFilter> {
        @Override
        public ResizableInvertibleBloomFilter decode(ByteBuf byteBuf) {
            int smallCellCount = ByteBufSerializer.int32.decode(byteBuf);
            InvertibleBloomFilter.Serializer ibfSerializer = new InvertibleBloomFilter.Serializer();
            InvertibleBloomFilter ibf = ibfSerializer.decode(byteBuf);
            Sizes size = determineSize(ibf, smallCellCount);

            ResizableInvertibleBloomFilter ribf =
                    new ResizableInvertibleBloomFilter(ibf.keyLengthsSum, smallCellCount, size);
            ribf.cells = ibf.cells;
            return ribf;
        }

        @Override
        public void encode(ResizableInvertibleBloomFilter ibf, ByteBuf byteBuf) {
            ByteBufSerializer.int32.encode(ibf.smallCellCount, byteBuf);
            InvertibleBloomFilter.Serializer ibfSerializer = new InvertibleBloomFilter.Serializer();
            ibfSerializer.encode(ibf, byteBuf);
        }


        //        @Override
        public String getName() {
            return "ResizableInvertibleBloomFilter";
        }


        private Sizes determineSize(InvertibleBloomFilter ibf, int smallCellCount) {
            Sizes size = null;
            for (Sizes possibleSize : SIZES_ATTEMPT_ORDER) {
                int possibleCellCount =
                        (int)
                                Arrays.stream(
                                        OneHashingBloomFilterUtils.resizingDivisors(
                                                K_INDEPENDENT_HASH_FUNCTIONS,
                                                smallCellCount,
                                                possibleSize.resizingFactors))
                                        .sum();

                if (possibleCellCount == ibf.cells.length) size = possibleSize;
            }
            if (size == null) throw new IllegalStateException("size not possible based on cell count");
            return size;
        }
    }

    /**
     * @see <a href="https://example.slab.com/posts/resizable-ibf-6bcc9tq4">Resizable IBF - Slab</a>
     */
    public enum Sizes {
        SMALL(1, 1, 1),
        MEDIUM(5, 6, 7),
        LARGE(23, 19, 17),
        XLARGE(5 * 23, 6 * 19, 7 * 17);

        public final int[] resizingFactors;

        Sizes(int... resizingFactors) {
            this.resizingFactors = resizingFactors;
        }

        public long[] divisors(int smallCellCount) {
            long[] divisors = new long[K_INDEPENDENT_HASH_FUNCTIONS];
            long[] primeDivisors =
                    OneHashingBloomFilterUtils.primeDivisors(K_INDEPENDENT_HASH_FUNCTIONS, smallCellCount);

            for (int i = 0; i < K_INDEPENDENT_HASH_FUNCTIONS; i++) {
                divisors[i] = resizingFactors[i] * primeDivisors[i];
            }

            return divisors;
        }

        public int cellCountFactor() {
            int min = resizingFactors[0];
            for (int i = 1; i < resizingFactors.length; i++) min = Math.min(min, resizingFactors[i]);
            return min;
        }
    }
}
