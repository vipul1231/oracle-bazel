package com.example.ibf;

import com.example.core.annotations.DataType;
import java.util.List;

public class SEInvertibleBloomFilter extends InvertibleBloomFilter {
    private final List<DataType> keyTypes;
    private final List<Integer> keyLengths;

    public SEInvertibleBloomFilter(List<DataType> keyTypes, List<Integer> keyLengths, int requestedCellCount) {
        super(IbfDbUtils.computeKeyLengthsSum(keyLengths), requestedCellCount);
        this.keyTypes = keyTypes;
        this.keyLengths = keyLengths;
    }

    /**
     * For strata estimator, verify if a cell is pure by checking the CRC32 hash of its keySums is equals to its
     * rowHashSum
     */
    @Override
    protected boolean isPureCell(int cellIndex) {
        Cell cell = getCell(cellIndex);
        if (cell == null) return false;
        if (!cell.isSingular()) return false;

        return StrataEstimatorUtils.md5Hash52bit(this.keyTypes, this.keyLengths, cell.keySums()) == cell.rowHashSum();
    }
}
