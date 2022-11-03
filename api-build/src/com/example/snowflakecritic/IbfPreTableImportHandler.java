package com.example.snowflakecritic;

import com.example.core.TableRef;

import java.sql.SQLException;

public class IbfPreTableImportHandler extends AbstractSnowflakeTableWorker implements PreTableImportHandler {

    public IbfPreTableImportHandler(SnowflakeConnectorServiceConfiguration serviceConfig, TableRef tableRef) {
        super(serviceConfig, tableRef);
    }

    @Override
    public void handlePreImport() {
        if (!isTableExcluded() && tableState.isPreImport()) {
            try {
                //LOG.info("Creating baseline IBF for table " + tableRef);

                serviceConfig.getIbfCheckpointManager(tableRef).reset();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
