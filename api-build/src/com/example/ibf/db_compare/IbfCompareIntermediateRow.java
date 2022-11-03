package com.example.ibf.db_compare;

import java.util.Arrays;
import java.util.Objects;

public class IbfCompareIntermediateRow {
    public final long[] key;
    public final long rowHash;
    public final String rowRepresentationMd5Hash;
    public final String rowRepresentation;

    public IbfCompareIntermediateRow(
            long[] key, long rowHash, String rowRepresentationMd5Hash, String rowRepresentation) {
        this.key = key;
        this.rowHash = rowHash;
        this.rowRepresentationMd5Hash = rowRepresentationMd5Hash;
        this.rowRepresentation = rowRepresentation;
    }

    @Override
    public String toString() {
        return "IbfCompareIntermediateRow{"
                + "keys="
                + Arrays.toString(key)
                + ", rowHash="
                + rowHash
                + ", rowRepresentationMd5Hash='"
                + rowRepresentationMd5Hash
                + '\''
                + ", rowRepresentation='"
                + rowRepresentation
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IbfCompareIntermediateRow that = (IbfCompareIntermediateRow) o;
        return rowHash == that.rowHash
                && Arrays.equals(key, that.key)
                && rowRepresentationMd5Hash.equals(that.rowRepresentationMd5Hash)
                && rowRepresentation.equals(that.rowRepresentation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowHash, rowRepresentationMd5Hash, rowRepresentation);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
