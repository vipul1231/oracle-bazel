package com.example.oracle;

import com.example.core.Column;
import com.example.db.spi.AbstractHashIdGenerator;

import java.util.List;
import java.util.Map;

public class HashIdGenerator implements AbstractHashIdGenerator {
    @Override
    public String hashId(Map<String, ?> row, List<Column> knownTypes) {
        return null;
    }
}
