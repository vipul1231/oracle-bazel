package com.example.snowflakecritic;

import com.example.core.TableRef;
import com.example.db.SyncerCommon;
import com.example.oracle.Pair;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSnowflakeTableWorker extends SnowflakeConfigurationAware {

    protected final TableRef tableRef;
    protected SnowflakeConnectorState.SnowflakeTableState tableState;
    protected final SnowflakeTableInfo tableInfo;
    protected final SnowflakeTypeCoercer snowflakeTypeCoercer = new SnowflakeTypeCoercer();

    public AbstractSnowflakeTableWorker(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig);
        this.tableRef = tableRef;
        this.tableState = serviceConfig.getConnectorState().getTableState(tableRef);
        this.tableInfo = getSnowflakeInformer().tableInfo(tableRef);
    }

    /**
     * Switch to the table's schema before querying the table.
     *
     * @param connection
     * @throws SQLException
     */
    protected void useSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE SCHEMA %s", tableRef.schema));
        }
    }

    protected void checkpoint() {
        getOutput().checkpoint(getSnowflakeConnectorState());
    }

    protected boolean isTableExcluded() {
        return getStandardConfig().excludedTables().containsKey(tableRef.toTableName());
    }

    protected Map<String, Object> handleTypePromotions(Map<String, Object> row) {

        Map<String, Object> values = new HashMap<>();
        for (SnowflakeColumnInfo columnInfo : tableInfo.includedColumnInfo()) {
            Object value = row.get(columnInfo.columnName);
            if (value != null) {
                Pair<Object, Boolean> promotedType = snowflakeTypeCoercer.promoteOrMinimize(columnInfo, value);
                if (promotedType.right) {
                    getOutput()
                            .promoteColumnType(
                                    tableInfo.tableDefinition(),
                                    columnInfo.columnName,
                                    columnInfo.asExampleColumn().asColumnType());
                }
                value = promotedType.left;
            }
            values.put(columnInfo.columnName, value);
        }

        return values;
    }
}
