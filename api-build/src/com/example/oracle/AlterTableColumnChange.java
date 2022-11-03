package com.example.oracle;

import com.example.core.ColumnType;
import com.example.core.SyncMode;
import com.example.core.TableRef;

import java.time.Instant;
import java.util.List;

public class AlterTableColumnChange {
    /**
     * @TODO: need to decide Type as per business logic
     */
    public Object op;

    public ColDataType columnType;

    public String defaultValue;

    public String columnName;

    /**
     * @TODO: Need to add business logic
     * @param ddlEvent
     * @return
     */
    public static List<AlterTableColumnChange> parse(String ddlEvent) {
        return null;
    }

    /**
     * @TODO: Need to add business logic
     * @param tableRef
     * @param syncMode
     * @param columnType
     * @param opTime
     * @return
     */
    public UpdateSchemaOperation toUpdateSchemaOperation(TableRef tableRef,
                                                         SyncMode syncMode,
                                                         ColumnType columnType,
                                                         Instant opTime) {

        return null;
    }
}
