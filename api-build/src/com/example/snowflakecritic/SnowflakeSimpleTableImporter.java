package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.core2.OperationTag;
import com.example.ibf.db_incremental_sync.IbfCheckpointManager;
import com.example.snowflakecritic.ibf.SnowflakeIbfAdapter;

import java.sql.SQLException;

/** The simple importer uses a basic select columns from table with no conditions strategy */
public class SnowflakeSimpleTableImporter extends AbstractSnowflakeTableImporter {

    public SnowflakeSimpleTableImporter(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
    }

    @Override
    protected String getTableQuery() {
        return SnowflakeSQLUtils.select(getSnowflakeInformer().tableInfo(tableRef).includedColumnInfo(), tableRef.name);
    }

    @Override
    protected String getImportMethodName() {
        return "QUERY_TABLE";
    }

    @Override
    protected OperationTag[] getOperationTags() {
        return new OperationTag[] {OperationTag.HISTORICAL_SYNC};
    }
}
