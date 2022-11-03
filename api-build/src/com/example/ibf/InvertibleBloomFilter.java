package com.example.ibf;

import io.netty.buffer.ByteBuf;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.function.Function;

public class InvertibleBloomFilter {
    //    protected Lazy<Cell[]> cells;
    protected Cell[] cells;

    public static final int K_INDEPENDENT_HASH_FUNCTIONS = 3;

    Function<Long, long[]> indicesHash;

    protected long[] divisors;

    // sum of primary keys' lengths
    protected int keyLengthsSum;

    boolean decoded = false;

    /**
     * Construct a new Invertible Bloom Filter
     *
     * @param keyLengthsSum      the lengths of the primary keys and the long[] for keySums
     * @param requestedCellCount the target number of cells in the IBF (i.e., the IBF size); due to how the one-hashing
     *                           indices hash scheme works, the IBF will have slightly more cells than requested
     */
    public InvertibleBloomFilter(int keyLengthsSum, int requestedCellCount) {
        this(keyLengthsSum, OneHashingBloomFilterUtils.primeDivisors(K_INDEPENDENT_HASH_FUNCTIONS, requestedCellCount));
    }

    InvertibleBloomFilter(int keyLengthsSum, long[] divisors) {
        this.keyLengthsSum = keyLengthsSum;
        this.divisors = divisors;

        this.indicesHash = OneHashingBloomFilterUtils.indexHashes(divisors);

//        this.cells =
//                new Lazy<>(
//                        () -> {
//                            Cell[] cells = new Cell[OneHashingBloomFilterUtils.totalCellCount(divisors)];
//                            for (int i = 0; i < cells.length; i++) {
//                                cells[i] = new Cell(new long[this.keyLengthsSum], 0L, 0L);
//                            }
//                            return cells;
//                        });
        this.cells =
                new Cell[OneHashingBloomFilterUtils.totalCellCount(divisors)];
        for (int i = 0; i < cells.length; i++) {
            cells[i] = new Cell(new long[this.keyLengthsSum], 0L, 0L);
        }
    }

    /**
     * load the cell located a cellIndex with the provided values
     */
    public void loadFromDatabase(int cellIndex, long[] keySums, long rowHashSum, long count) {
        getCell(cellIndex).load(keySums, rowHashSum, count);
    }

    /**
     * Insert an element into the IBF
     *
     * @param rowHashSum sum of the hash of a row
     * @param keySums    long[] of length elementLength
     */
    public void insert(long[] keySums, long rowHashSum) {
        for (int cellIndex : distinctHashIndices(rowHashSum)) {
            getCell(cellIndex).insert(keySums, rowHashSum);
        }
    }

    /**
     * Remove an element from the IBF
     *
     * @param keySums    long[] of length keyLength
     * @param rowHashSum sum of the hash of a row
     */
    public void remove(long[] keySums, long rowHashSum) {
        for (int cellIndex : distinctHashIndices(rowHashSum)) {
            getCell(cellIndex).remove(keySums, rowHashSum);
        }
    }

    /**
     * Compute the differences in the set represented by this IBF from the set represented by other IBF. This is a
     * probabilistic operation that can fail based on the parameters of the IBFs and the number of differences.
     *
     * @param otherIBF - the other ibf
     * @return IBFDecodeResult the result of the comparison
     */
    public IBFDecodeResult compare(InvertibleBloomFilter otherIBF) {
        return this.subtract(otherIBF).decode();
    }

    /**
     * List all the elements represented in the InvertibleBloomFilter. This is normally used on the difference
     * InvertibleBloomFilter that resulted from a subtract() operation.
     *
     * <p>This is a probablistic operation that can fail when the number of represented elements larger than the
     * InvertibleBloomFilter's capacity, which is proportional the IBF cell count.
     */
    public IBFDecodeResult decode() {
        IBFDecodeResult result = new IBFDecodeResult();
        Deque<Integer> pureList = new ArrayDeque<>();
        this.decoded = true;

        while (true) {
            if (pureList.isEmpty()) {
                for (int i = 0; i < cells.length; i++) {
                    if (isPureCell(i)) pureList.add(i);

                    // set zero cells to null to allow GC to reclaim memory if needed
                    Cell cell = getCell(i);
                    if (cell != null && cell.isZero()) cells[i] = null;
                }
                if (pureList.isEmpty()) break;
            }

            int index = pureList.pop();
            if (!isPureCell(index)) continue;

            Cell pureCell = getCell(index).copy();
            if (pureCell.getCount() > 0) {
                result.aWithoutB.add(new IBFDecodeResultElement(pureCell.keySums(), pureCell.rowHashSum()));
            } else {
                result.bWithoutA.add(new IBFDecodeResultElement(pureCell.keySums(), pureCell.rowHashSum()));
            }

            for (int cellIndex : distinctHashIndices(pureCell.rowHashSum())) {
                Cell cell = getCell(cellIndex);
                if (cell == null || cell.isZero()) continue;

                cell.subtract(pureCell);
            }
        }
        for (Cell cell : cells) {
            if (!(cell == null || cell.isZero())) {
                result.succeeded = false;
                return result;
            }
        }
        result.succeeded = true;
        return result;
    }

    /**
     * Subtract another InvertibleBloomFilter from this instance
     *
     * @param other the other InvertibleBloomFilter
     * @return a new InvertibleBloomFilter with the difference
     */
    public InvertibleBloomFilter subtract(InvertibleBloomFilter other) {
        validateSubtractArg(other);

        for (int i = 0; i < cells.length; i++) {
            cells[i] = cells[i].copy();
            cells[i].subtract(other.cells[i]);
        }

        return this;
    }

    /**
     * - * @return returns true the accumulators in this cell only represent the insertion or deletion of a single
     * element. - * When a cell is Pure, the value of the inserted or deleted element can be returned. The count value
     * indicates - * the operation: 1 implies the element was inserted, -1 implies deleted. + * @return returns true
     * when the cell has a count of 1 or -1, indicate that it *may* represent a single element
     */
    protected boolean isPureCell(int cellIndex) {
        if (getCell(cellIndex) == null) return false;
        if (!getCell(cellIndex).isSingular()) return false;

        int[] hashIndices = distinctHashIndices(getCell(cellIndex).rowHashSum());

        for (int i = 0; i < K_INDEPENDENT_HASH_FUNCTIONS; i++) {
            if (hashIndices[i] == cellIndex) return true;
        }

        return false;
    }

    public Cell getCell(int cellIndex) {
        return cells[cellIndex];
    }

    public Cell[] getCells() {
        return cells;
    }

    public int dataSize() {
        return cells.length * (8 * (keyLengthsSum() + 2));
    }

    public int keyLengthsSum() {
        return keyLengthsSum;
    }

    @Override
    public String toString() {
        return Arrays.toString(cells);
    }

    private int[] distinctHashIndices(long rowHashSum) {
        return Arrays.stream(indicesHash.apply(rowHashSum)).mapToInt(Math::toIntExact).toArray();
    }

    private void validateSubtractArg(InvertibleBloomFilter other) {
        if (cells.length != other.cells.length)
            throw new IllegalArgumentException(
                    String.format("size mismatch: %d != %d", cells.length, other.cells.length));

        if (keyLengthsSum != other.keyLengthsSum)
            throw new IllegalArgumentException(
                    String.format("key length mismatch: %d != %d", keyLengthsSum, other.keyLengthsSum));
    }

    /**
     * NOTE: If you are going to modify this serializer, please make sure to add a way to migrate all the stored IBFs.
     * Otherwise, all the existing connectors that use Ibf will be broken.
     */
    public static class Serializer extends ByteBufSerializer<InvertibleBloomFilter> {

        //        @Override
        public InvertibleBloomFilter decode( ByteBuf byteBuf) {
            int keyLengthSum = ByteBufSerializer.int32.decode(byteBuf);
            long[] divisors = new long[K_INDEPENDENT_HASH_FUNCTIONS];
            for (int i = 0; i < K_INDEPENDENT_HASH_FUNCTIONS; i++) {
                divisors[i] = ByteBufSerializer.long64.decode(byteBuf);
            }

            InvertibleBloomFilter ibf = new InvertibleBloomFilter(keyLengthSum, divisors);
            for (int cellIndex = 0; cellIndex < ibf.cells.length; cellIndex++) {
                long[] keySums = new long[keyLengthSum];
                for (int keyIndex = 0; keyIndex < keyLengthSum; keyIndex++) {
                    keySums[keyIndex] = ByteBufSerializer.long64.decode(byteBuf);
                }
                long rowHashSum = ByteBufSerializer.long64.decode(byteBuf);
                long count = ByteBufSerializer.long64.decode(byteBuf);

                ibf.loadFromDatabase(cellIndex, keySums, rowHashSum, count);
            }

            return ibf;
        }

        @Override
        public void encode(InvertibleBloomFilter ibf,  ByteBuf byteBuf) {
            ByteBufSerializer.int32.encode(ibf.keyLengthsSum, byteBuf);
            for (int i = 0; i < K_INDEPENDENT_HASH_FUNCTIONS; i++) {
                ByteBufSerializer.long64.encode(ibf.divisors[i], byteBuf);
            }

            for (int cellIndex = 0; cellIndex < ibf.cells.length; cellIndex++) {
                for (int keyIndex = 0; keyIndex < ibf.keyLengthsSum; keyIndex++) {
                    ByteBufSerializer.long64.encode(ibf.getCell(cellIndex).keySums()[keyIndex], byteBuf);
                }
                ByteBufSerializer.long64.encode(ibf.getCell(cellIndex).rowHashSum(), byteBuf);
                ByteBufSerializer.long64.encode(ibf.getCell(cellIndex).getCount(), byteBuf);
            }
        }

        
//        @Override
        public String getName() {
            return "InvertibleBloomFilter";
        }
    }
}
