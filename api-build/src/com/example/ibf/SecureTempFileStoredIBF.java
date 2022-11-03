package com.example.ibf;


import com.example.crypto.Encrypt;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.UUID;

import static com.example.ibf.ResizableInvertibleBloomFilter.computeDivisorPartitionSum;
import static com.example.ibf.ResizableInvertibleBloomFilter.convertCellIndex;

/**
 * Wraps an IBF then stores the IBF's cells on disk until the cells are needed *
 */
public class SecureTempFileStoredIBF<IBFType extends InvertibleBloomFilter> {
    private static final SecretKey SECRET_KEY = Encrypt.newEphemeralKey();

    private final IBFType ibf;
    private final int cellCount;
    private final int dataSize;

    private final SecureTempFile file;
    private final Cell.Serializer cellSerializer;

    public SecureTempFileStoredIBF(IBFType value) {
        this.ibf = value;
        this.cellCount = value.cells.length;
        this.dataSize = value.dataSize();

        try {
            this.file = new SecureTempFile(UUID.randomUUID().toString(), SECRET_KEY, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.cellSerializer = new Cell.Serializer(value.keyLengthsSum());

        flush();
    }

    public int getDataSize() {
        return dataSize;
    }

    /**
     * Retrieve the IBF object
     *
     * @return InvertibleBloomFilter (or descendants) - the fully loaded, in-memory IBF object
     */
    public IBFType getIBF() {
        if (!cellsLoaded()) restore();

        return ibf;
    }

    /**
     * Perform the subtract operation on 2 IBFs without loading the IBFs' cells into memory
     *
     * @param term1
     * @param term2
     * @return an (in-memory) InvertibleBloomFilter object
     */
    public static InvertibleBloomFilter subtract(
            SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> term1,
            SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> term2) {
        InvertibleBloomFilter diffIBF = new InvertibleBloomFilter(term1.ibf.keyLengthsSum, term1.ibf.divisors);
        Iterator<Cell> term1iterator = term1.iterator();
        Iterator<Cell> term2iterator = term2.iterator();

        for (int cellIndex = 0; term1iterator.hasNext() && term2iterator.hasNext(); cellIndex++) {
            diffIBF.cells[cellIndex] = term1iterator.next();
            diffIBF.cells[cellIndex].subtract(term2iterator.next());
        }

        return diffIBF;
    }

    /**
     * Perform the resize operation on a ResizableIBF without loading the original IBF's cells into memory
     *
     * @param resizable
     * @param size
     * @return SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> - the resized IBF stored on disk
     */
    public static SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> resize(
            SecureTempFileStoredIBF<ResizableInvertibleBloomFilter> resizable,
            ResizableInvertibleBloomFilter.Sizes size) {

        ResizableInvertibleBloomFilter resized =
                new ResizableInvertibleBloomFilter(resizable.ibf.keyLengthsSum, resizable.ibf.smallCellCount, size);

        long[] divisorPartitionSum = computeDivisorPartitionSum(resizable.ibf.divisors);
        long[] newDivisorPartitionSum = computeDivisorPartitionSum(resized.divisors);

        {
            int cellIndex = 0;
            for (Iterator<Cell> iterator = resizable.iterator(); iterator.hasNext(); cellIndex++) {
                Cell cell = iterator.next();
                if (cell.isZero()) continue;

                resized.cells
                        [
                        convertCellIndex(resized.divisors, divisorPartitionSum, newDivisorPartitionSum, cellIndex)]
                        .add(cell);
            }
        }

        return new SecureTempFileStoredIBF<>(resized);
    }

    private void flush() {
        if (!cellsLoaded()) return;

        try (OutputStreamWriter writer = file.getWriter()) {
            for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
                System.out.println("ibf:" + ibf);
//                writer.write(Base64.getEncoder().encodeToString(cellSerializer.encode(ibf.getCell(cellIndex))) + "\n");
            }

            // TODO check
//            ibf.cells = null;
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void restore() {
        if (cellsLoaded()) return;

//        ibf.cells = new Lazy<>(() -> new Cell[cellCount]);
        ibf.cells = new Cell[cellCount];

        int cellIndex = 0;
        for (Iterator<Cell> iterator = iterator(); iterator.hasNext(); cellIndex++) {
            ibf.cells[cellIndex] = iterator.next();
        }
    }

    private Iterator<Cell> iterator() {
        return new Iterator<Cell>() {
            private final Iterator<String> delegate = file.iterator(); // backing iterator

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Cell next() {
//                return cellSerializer.decode(Base64.getDecoder().decode(delegate.next()));
                return null;
            }
        };
    }

    private boolean cellsLoaded() {
        return ibf.cells != null;
    }
}
