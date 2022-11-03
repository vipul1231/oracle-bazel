package com.example.ibf;

import com.example.core.annotations.DataType;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StrataEstimatorDecodeResult extends DecodeResult {
    public int count;

    private List<DataType> keyTypes;
    private List<Integer> keyLengths;

    public StrataEstimatorDecodeResult(List<DataType> keyTypes, List<Integer> keyLengths) {
        this.keyTypes = keyTypes;
        this.keyLengths = keyLengths;
    }

    public Set<List<Object>> sampleAKeys() {
        return aWithoutB
                .stream()
                .map(element -> IbfDbUtils.decodePk(keyTypes, keyLengths, element.keySum))
                .collect(Collectors.toSet());
    }

    public Set<List<Object>> sampleBKeys() {
        Set<List<Object>> bKeys =
                bWithoutA
                        .stream()
                        .map(element -> IbfDbUtils.decodePk(keyTypes, keyLengths, element.keySum))
                        .collect(Collectors.toSet());
        sampleAKeys().forEach(bKeys::remove);
        return bKeys;
    }

    @Override
    public String toString() {
        return "StrataEstimatorDecodeResult { count="
                + count
                + " aWithoutB="
                + listToStringWithTruncate(aWithoutB)
                + " bWithoutA="
                + listToStringWithTruncate(bWithoutA)
                + " }";
    }
}
