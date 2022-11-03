package com.example.ibf;

import com.example.core.Column;
import com.example.core.SyncMode;
import com.example.core.annotations.DataType;
import com.example.oracle.Names;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

public class StrataEstimatorUtils {

    private StrataEstimatorUtils() {
    }

    public static long md5Hash52bit(List<DataType> primaryKeyTypes, List<Integer> keyLengths, long[] keySums) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        List<Object> keys = IbfDbUtils.decodePk(primaryKeyTypes, keyLengths, keySums);
        String concatKeyValue = StrataEstimatorUtils.concatKeyValues(keys);
        String keyInHex = Hex.encodeHexString(md.digest(concatKeyValue.getBytes())).substring(0, 13); // 52-bit hash
        return Long.parseLong(keyInHex, 16);
    }

    public static String syncMode(List<Column> columns) {
        return syncModeFromColumnNames(columns.stream().map(c -> c.name).collect(Collectors.toList()));
    }

    public static String syncModeFromColumnNames(List<String> columnNames) {
        for (String name : columnNames) {
            switch (name) {
                case Names.example_DELETED_COLUMN:
                    return SyncMode.Legacy.name();
                case Names.example_ACTIVE:
                case Names.example_START:
                case Names.example_END:
                    return SyncMode.History.name();
            }
        }
        return "";
    }

    public static String concatKeyValues(List<Object> keys) {
        StringBuilder concatKeyValue = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            concatKeyValue.append(keys.get(i).toString());
            if (i + 1 != keys.size()) concatKeyValue.append("|");
        }
        return concatKeyValue.toString();
    }
}
