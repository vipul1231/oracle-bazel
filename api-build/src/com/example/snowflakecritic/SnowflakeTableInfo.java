package com.example.snowflakecritic;

import com.example.core.Column;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.db.DbTableInfo;

import java.util.ArrayList;
import java.util.List;

public class SnowflakeTableInfo extends DbTableInfo<SnowflakeColumnInfo> {
    private boolean changeTrackingEnabled = false;

    public SnowflakeTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            List<SnowflakeColumnInfo> sourceColumnInfo,
            int originalColumnCount,
            SyncMode syncMode) {
        super(sourceTableRef, schemaPrefix, sourceColumnInfo, originalColumnCount, syncMode);

    }

    public SnowflakeTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            Long estimatedRowCount,
            Long estimatedDataBytes,
            List<SnowflakeColumnInfo> sourceColumnInfo,
            int originalColumnCount,
            SyncMode syncMode) {
        super(
                sourceTableRef,
                schemaPrefix,
                estimatedRowCount,
                estimatedDataBytes,
                sourceColumnInfo,
                originalColumnCount,
                syncMode);
    }

    @Override
    protected List<Column> initexampleColumns(boolean hasPrimaryKey) {
        return new ArrayList<>();
    }

    public boolean isChangeTrackingEnabled() {
        return changeTrackingEnabled;
    }

    public void setChangeTrackingEnabled(boolean changeTrackingEnabled) {
        this.changeTrackingEnabled = changeTrackingEnabled;
    }

    public void updateTableDefinition() {
        renewTableDefinition();
    }
}
