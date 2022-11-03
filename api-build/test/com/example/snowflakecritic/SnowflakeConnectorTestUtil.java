package com.example.snowflakecritic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.ConnectionParameters;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SnowflakeConnectorTestUtil {

    public static final String ID = "ID";
    public static final String TEXT_COL = "TEXT_COL";
    public static final String DATE_COL = "DATE_COL";
    public static final String FLOAT_COL = "FLOAT_COL";
    public static final String BOOLEAN_COL = "BOOLEAN_COL";
    public static final String TIME_COL = "TIME_COL";
    public static final String TIMESTAMP_NTZ_COL = "TIMESTAMP_NTZ_COL";
    public static final String TIMESTAMP_TZ_COL = "TIMESTAMP_TZ_COL";
    public static final String TIMESTAMP_LTZ_COL = "TIMESTAMP_LTZ_COL";
    public static final String VARIANT_COL = "VARIANT_COL";
    public static final String BINARY_COL = "BINARY_COL";

    public static SnowflakeTableInfoBuilder tableInfoBuilder(String schema, String tableName) {
        return new SnowflakeTableInfoBuilder(schema, tableName);
    }

    public static SnowflakeSourceCredentials newCredentials(String host, int port, String db) {
        SnowflakeSourceCredentials credentials = new SnowflakeSourceCredentials();
        credentials.host = host;
        credentials.port = port;
        credentials.database = Optional.of(db);
        return credentials;
    }

    public static SnowflakeInformer mockInformer(SnowflakeTableInfo... tableInfos) {
        SnowflakeInformer informer = mock(SnowflakeInformer.class);

        StandardConfig mockStdConfig = mockStandardConfig(tableInfos);
        when(informer.getStandardConfig()).thenReturn(mockStdConfig);

        for (SnowflakeTableInfo tableInfo : tableInfos) {
            when(informer.tableInfo(tableInfo.sourceTable)).thenReturn(tableInfo);
        }

        return informer;
    }

    public static Set<TableRef> includedTables(SnowflakeTableInfo... tableInfos) {
        Set<TableRef> includedTables = new HashSet<>();
        for (SnowflakeTableInfo tableInfo : tableInfos) {
            includedTables.add(tableInfo.sourceTable);
        }
        return includedTables;
    }

    public static SnowflakeTableInfo allTypesTestTable(String schema, String name, String prefix) {
        return tableInfoBuilder(schema, name)
                .schemaPrefix(prefix)
                .pkCol(ID, SnowflakeType.NUMBER)
                .col(TEXT_COL, SnowflakeType.TEXT)
                .col(DATE_COL, SnowflakeType.DATE)
                .col(TIME_COL, SnowflakeType.TIME)
                .col(FLOAT_COL, SnowflakeType.FLOAT)
                .col(TIMESTAMP_NTZ_COL, SnowflakeType.TIMESTAMP_NTZ)
                .col(TIMESTAMP_TZ_COL, SnowflakeType.TIMESTAMP_TZ)
                .col(TIMESTAMP_LTZ_COL, SnowflakeType.TIMESTAMP_LTZ)
                .col(BINARY_COL, SnowflakeType.BINARY)
                .col(BOOLEAN_COL, SnowflakeType.BOOLEAN)
                .build();
    }

    public static StandardConfig mockStandardConfig(SnowflakeTableInfo... tableInfos) {
        StandardConfig standardConfig = mock(StandardConfig.class);
        when(standardConfig.excludedTables()).thenReturn(new HashMap<>());
        return standardConfig;
    }

    public static SnowflakeSourceCredentials testCredentials(SnowflakeSourceCredentials.UpdateMethod updateMethod) {
        SnowflakeSourceCredentials credentials = new SnowflakeSourceCredentials();
        credentials.updateMethod = updateMethod;
        credentials.port = 0;
        credentials.database = Optional.of("dummy");
        return credentials;
    }

    public static ConnectionParameters testConnectionParams() {
        return new ConnectionParameters("owner", "prefix", TimeZone.getDefault());
    }
}
