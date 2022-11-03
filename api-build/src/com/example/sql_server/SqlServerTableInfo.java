package com.example.sql_server;

import com.example.core.Column;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.db.DbTableInfo;
import com.google.common.collect.Lists;
import java.util.*;
import java.util.stream.Collectors;

public class SqlServerTableInfo extends DbTableInfo<SqlServerColumnInfo> {

    final Set<ExcludeReason> excludeReasons = new HashSet<>();
    final SqlServerChangeType changeType;
    final String cdcCaptureInstance;
    final boolean hasClusteredIndex;

    Long minChangeVersion;
    private Set<SqlServerColumnInfo> includedColumnInfo;

    public SqlServerTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            long estimatedRowCount,
            long estimatedDataBytes,
            List<SqlServerColumnInfo> sourceColumnInfo,
            int originalColumnCount,
            Set<ExcludeReason> excludeReasons,
            SqlServerChangeType changeType,
            String cdcCaptureInstance,
            boolean hasClusteredIndex,
            Long changeVersion,
            SyncMode syncMode) {
        super(
                sourceTableRef,
                schemaPrefix,
                estimatedRowCount,
                estimatedDataBytes,
                sourceColumnInfo,
                originalColumnCount,
                syncMode);
        this.excludeReasons.addAll(excludeReasons);
        this.changeType = changeType;
        this.cdcCaptureInstance = cdcCaptureInstance;
        this.hasClusteredIndex = hasClusteredIndex;
        this.minChangeVersion = changeVersion;

        // We need to exclude columns not present in CDC instance,
        // so renew the table definition after change type is set
        includedColumnInfo = null;
        renewTableDefinition();
    }

    public SqlServerTableInfo(
            TableRef sourceTableRef,
            String schemaPrefix,
            long estimatedRowCount,
            long estimatedDataBytes,
            int originalColumnCount) {
        this(
                sourceTableRef,
                schemaPrefix,
                estimatedRowCount,
                estimatedDataBytes,
                Collections.emptyList(),
                originalColumnCount,
                Collections.emptySet(),
                SqlServerChangeType.NONE,
                "",
                false,
                null,
                SyncMode.Legacy);
    }

    @Override
    public Set<SqlServerColumnInfo> includedColumnInfo() {
        if (includedColumnInfo == null) includedColumnInfo = fetchIncludedColumnInfo();

        return Collections.unmodifiableSet(includedColumnInfo);
    }

    private Set<SqlServerColumnInfo> fetchIncludedColumnInfo() {
        return super.includedColumnInfo()
                .stream()
                .filter(ci -> changeType != SqlServerChangeType.CHANGE_DATA_CAPTURE || ci.includedInCDC)
                .collect(Collectors.toSet());
    }

    @Override
    protected List<Column> initexampleColumns(boolean hasPrimaryKey) {
        List<Column> exampleColumns = Lists.newArrayList();

        if (!hasPrimaryKey) {
//            exampleColumns.add(Column.String(Names.example_ID_COLUMN).primaryKey(true).build());
        }
        return exampleColumns;
    }
}
