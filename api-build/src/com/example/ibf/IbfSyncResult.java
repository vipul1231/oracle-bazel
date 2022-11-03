package com.example.ibf;

import com.example.core.annotations.DataType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class IbfSyncResult {

    private final List<DataType> primaryKeyTypes;
    private final List<Integer> keyLengths;
    @VisibleForTesting public final IBFDecodeResult ibfDecodeResult;

    @VisibleForTesting
    public IbfSyncResult(List<DataType> primaryKeyTypes, List<Integer> keyLengths) {
        this(new IBFDecodeResult(), primaryKeyTypes, keyLengths);
    }

    // for backward compatibility
    @Deprecated
    public IbfSyncResult(IBFDecodeResult ibfDecodeResult, DataType primaryKeyType, int keyLength) {
        this(ibfDecodeResult, ImmutableList.of(primaryKeyType), ImmutableList.of(keyLength));
    }

    public IbfSyncResult(
            IBFDecodeResult ibfDecodeResult, List<DataType> primaryKeyTypes, List<Integer> keyLengths) {
        this.ibfDecodeResult = ibfDecodeResult;
        this.primaryKeyTypes = primaryKeyTypes;
        this.keyLengths = keyLengths;
    }

    /** @return List<List<Object>> containing the sorted list of primary keys of rows that were inserted or updated */
    public List<List<Object>> upserts() {
        return ibfDecodeResult
                .aWithoutB
                .stream()
                .map(element -> IbfDbUtils.decodePk(primaryKeyTypes, keyLengths, element.keySum))
                // sorting the primary keys causes rows from the same page in the database to be fetched together
                .sorted(new ListComparator<>())
                .collect(Collectors.toList());
    }

    /** @return Set<List<Object></Object>> containing the primary keys of rows that were deleted */
    public Set<List<Object>> deletes() {
        Set<List<Object>> bKeys =
                ibfDecodeResult
                        .bWithoutA
                        .stream()
                        .map(element -> IbfDbUtils.decodePk(primaryKeyTypes, keyLengths, element.keySum))
                        .collect(Collectors.toSet());
        upserts().forEach(bKeys::remove);
        return bKeys;
    }

    public boolean getSucceeded() {
        return ibfDecodeResult.succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        ibfDecodeResult.succeeded = succeeded;
    }

    public List<IBFDecodeResultElement> aWithoutB() {
        return ibfDecodeResult.aWithoutB;
    }

    public List<IBFDecodeResultElement> bWithoutA() {
        return ibfDecodeResult.bWithoutA;
    }

    public long dataSize() {
        return ibfDecodeResult.dataSize();
    }

    class ListComparator<T> implements Comparator<List<T>> {

        @Override
        public int compare(List<T> l1, List<T> l2) {
            for (int i = 0; i < l1.size(); i++) {
                int result = l1.get(i).toString().compareTo(l2.get(i).toString());
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}
