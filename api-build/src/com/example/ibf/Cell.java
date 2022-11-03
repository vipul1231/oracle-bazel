package com.example.ibf;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

public class Cell {

    private long rowHashSum;
    private long[] keySums;
    private long count;

    /**
     * Construct a new cell from the provided values and hash function. The checkSumHash
     *
     * @param keySums    accumulates the sum of all key elements inserted into this cell
     * @param rowHashSum accumulates the sum of the hash of a row inserted into this cell
     * @param count      the number of elements that have been inserted into this cell; count can be negative when the cell
     *                   contains deleted elements
     */
    public Cell(long[] keySums, long rowHashSum, long count) {
        this.rowHashSum = rowHashSum;
        this.keySums = keySums;
        this.count = count;
    }

    /**
     * Replace all values in the cell with the values provided as arguments
     *
     * @param keySums
     * @param rowHashSum
     * @param count
     */
    public void load(long[] keySums, long rowHashSum, long count) {
        this.rowHashSum = rowHashSum;
        this.keySums = keySums;
        this.count = count;
    }

    /**
     * @return returns true when all values in the cell is empty, meaning all values are 0 and the cell contains no
     * inserted or removed elements.
     */
    public boolean isZero() {
        for (long key : keySums) {
            if (key != 0) {
                return false;
            }
        }
        return count == 0 && rowHashSum == 0;
    }

    /**
     * @return returns true when the cell has a count of 1 or -1, indicate that it *may* represent a single element
     */
    public boolean isSingular() {
        return count == 1L || count == -1;
    }

    /**
     * Insert an element into the cell
     *
     * @param keySums    long[] representing the element; must have the same array length as the elementSum
     * @param rowHashSum accumulates the sum of the hash of a row inserted into this cell
     */
    public void insert(long[] keySums, long rowHashSum) {
        for (int i = 0; i < keySums.length; i++) {
            this.keySums[i] ^= keySums[i];
        }
        this.rowHashSum ^= rowHashSum;
        count++;
    }

    /**
     * Remove an element from the cell
     *
     * @param keySums long[] representing the element; must have the same array length as the elementSum
     */
    public void remove(long[] keySums, long rowHashSum) {
        for (int i = 0; i < keySums.length; i++) {
            this.keySums[i] ^= keySums[i];
        }
        this.rowHashSum ^= rowHashSum;
        count--;
    }

    /**
     * Add the values in another cell to this cell's values. Equivalent to performing a insert() on this cell for all
     * the elements represented in other cell
     *
     * @param other the other cell
     */
    public void add(Cell other) {
        for (int i = 0; i < other.keySums.length; i++) {
            this.keySums[i] ^= other.keySums[i];
        }
        this.rowHashSum ^= other.rowHashSum;
        count += other.count;
    }

    /**
     * Subtract the values in another cell from this cell's values. Equivalent to performing a remove() on this cell for
     * all the elements represented in other cell
     *
     * @param other the other cell
     */
    public void subtract(Cell other) {
        for (int i = 0; i < other.keySums.length; i++) {
            this.keySums[i] ^= other.keySums[i];
        }
        this.rowHashSum ^= other.rowHashSum;
        this.count -= other.count;
    }

    public long[] keySums() {
        return keySums;
    }

    public long getCount() {
        return count;
    }

    public long rowHashSum() {
        return rowHashSum;
    }

    public Cell copy() {
        return new Cell(Arrays.copyOf(keySums(), keySums().length), rowHashSum(), getCount());
    }

    @Override
    public String toString() {
        return "Cell {rowHashSum=" + rowHashSum + ", keySums=" + Arrays.toString(keySums) + ", count=" + count + "}";
    }

    public static class Serializer extends ByteBufSerializer<Cell> {
        private final int keySumsLength;

        public Serializer(int keySumsLength) {
            this.keySumsLength = keySumsLength;
        }

        @Override
        public Cell decode(ByteBuf byteBuf) {
            long[] keySums = new long[keySumsLength];
            for (int keyIndex = 0; keyIndex < keySumsLength; keyIndex++) {
                keySums[keyIndex] = ByteBufSerializer.long64.decode(byteBuf);
            }
            return new Cell(
                    keySums, ByteBufSerializer.long64.decode(byteBuf), ByteBufSerializer.long64.decode(byteBuf));
        }

        @Override
        public void encode(Cell cell, ByteBuf byteBuf) {
            for (int keyIndex = 0; keyIndex < cell.keySums.length; keyIndex++) {
                ByteBufSerializer.long64.encode(cell.keySums()[keyIndex], byteBuf);
            }
            ByteBufSerializer.long64.encode(cell.rowHashSum(), byteBuf);
            ByteBufSerializer.long64.encode(cell.getCount(), byteBuf);
        }


        //        @Override
        public String getName() {
            return "Cell";
        }
    }
}
