package com.example.snowflakecritic.ibf;

import java.util.*;

import static com.example.db.DbRowValue.PRIMARY_KEY_NAME;
import static com.example.db.DbRowValue.UPDATABLE_COLUMN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.snowflakecritic.SnowflakeColumnInfo;
import com.example.snowflakecritic.SnowflakeInformationSchemaDao;
import com.example.snowflakecritic.SnowflakeTableInfo;
import com.example.snowflakecritic.SnowflakeType;
import org.junit.Test;

public class SnowflakeInformationSchemaDaoSpec extends SnowflakeIbfSpec {
    @Test
    public void fetchTableInfo_returnsExpectedInfo() {
        SnowflakeInformationSchemaDao informationSchemaQuerier =
                new SnowflakeInformationSchemaDao(snowflakeSource, new StandardConfig(), new HashMap<>());

        Map<TableRef, SnowflakeTableInfo> actualTableInfo = informationSchemaQuerier.fetchTableInfo();

        assertEquals(3, actualTableInfo.size());
        assertTrue(actualTableInfo.containsKey(INT_PK_TABLE));
        assertTrue(actualTableInfo.containsKey(STRING_PK_TABLE));
        assertTrue(actualTableInfo.containsKey(COMPOUND_PK_TABLE));

        assertEquals(2, actualTableInfo.get(INT_PK_TABLE).sourceColumns().size());
        assertEquals(2, actualTableInfo.get(STRING_PK_TABLE).sourceColumns().size());
        assertEquals(3, actualTableInfo.get(COMPOUND_PK_TABLE).sourceColumns().size());

        List<SnowflakeColumnInfo> expectedIntPkTableColumnInfo =
                List.of(
                        new SnowflakeColumnInfo(
                                INT_PK_TABLE,
                                PRIMARY_KEY_NAME,
                                1,
                                SnowflakeType.NUMBER,
                                DataType.BigDecimal,
                                OptionalInt.empty(),
                                OptionalInt.of(38),
                                OptionalInt.of(0),
                                true,
                                true,
                                false,
                                Optional.empty(),
                                false, false),
                        new SnowflakeColumnInfo(
                                INT_PK_TABLE,
                                UPDATABLE_COLUMN_NAME,
                                2,
                                SnowflakeType.TEXT,
                                DataType.String,
                                OptionalInt.of(32 * 4),
                                OptionalInt.empty(),
                                OptionalInt.empty(),
                                true,
                                false,
                                false,
                                Optional.empty(),
                                false, false));

        Collection<SnowflakeColumnInfo> actualIntPkTableColumnInfo =
                actualTableInfo.get(INT_PK_TABLE).sourceColumnInfo();
        assertTrue(actualIntPkTableColumnInfo.containsAll(expectedIntPkTableColumnInfo));
    }
}
