package com.example.ibf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DecodeResult {
    public final List<IBFDecodeResultElement> aWithoutB = new ArrayList<>();
    public final List<IBFDecodeResultElement> bWithoutA = new ArrayList<>();

    protected String listToStringWithTruncate(List<IBFDecodeResultElement> list) {
        int limit = 5;

        String printed =
                list.stream()
                        .limit(limit)
                        .map(IBFDecodeResultElement::toString)
                        .collect(Collectors.joining(",", "{", ""));

        if (list.size() > limit) return printed + ", ..." + list.size() + " total items... }";
        else return printed + "}";
    }
}
