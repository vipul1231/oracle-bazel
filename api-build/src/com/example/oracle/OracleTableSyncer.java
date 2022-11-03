package com.example.oracle;

import com.example.core.TableRef;
import com.example.db.Retrier;
import com.example.logger.ExampleLogger;
import oracle.jdbc.pool.OracleDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Base abstraction responsible for syncing a single table. Initially created to support ibf but can be used for
 * initial imports and extended to support logminer incremental updates in the future (i.e. refactoring, not rewriting).
 */
public abstract class OracleTableSyncer {
    protected static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    protected final OracleTableState tableState;
    protected final OracleTableInfo tableInfo;
    protected final OracleDataSource oracleDataSource;
    protected final OracleOutputHelper outputHelper;
    protected final OracleApi oracleApi;
    protected final DbObjectValidator dbObjectValidator;
    private final ExtractVolumeRecorder extractVolumeRecorder;

    protected Instant syncTime;

    public OracleTableSyncer(
            OracleApi oracleApi,
            OracleTableState tableState,
            OracleTableInfo tableInfo,
            DataSource dataSource,
            OracleOutputHelper outputHelper,
            DbObjectValidator dbObjectValidator) {
        this.oracleApi = oracleApi;
        this.tableState = tableState;
        this.tableInfo = tableInfo;
        this.oracleDataSource = (OracleDataSource) dataSource;
        this.outputHelper = outputHelper;
        this.dbObjectValidator = dbObjectValidator;
        this.extractVolumeRecorder =
                new ExtractVolumeRecorder(outputHelper.metricsHelper, outputHelper.output, "OracleIncrementalChanges");
    }

    protected void resyncTable(String reason) {
        /**
        LOG.customerWarning(
                ForcedResyncEvent.ofTable(
                        tableInfo.getTableRef().schema,
                        tableInfo.getTableRef().name,
                        ForcedResyncEvent.ResyncReason.STRATEGY,
                        reason));
         **/

        tableState.reset();
    }

    protected void upsert(Map<String, Object> row, Instant changeTime) {
        submitRecord(ChangeType.INSERT, row, changeTime);
    }

    private void submitRecord(ChangeType type, Map<String, Object> row, Instant changeTime) {
        outputHelper.submitRecord(row, tableInfo.getTableRef(), tableInfo.getColumns(), changeTime, type);
        extractVolumeRecorder.updateExtractVolume(row);
    }

    protected void delete(Map<String, Object> key, Instant changeTime) {
        Map<String, Object> row = new HashMap<>(key);

        tableInfo
                .getPrimaryKeys()
                .forEach(
                        columnInfo -> {
                            if (columnInfo.getSourceColumn().oracleType.isNumber()) {
                                row.put(
                                        columnInfo.getName(),
                                        outputHelper.minimizeNumber(
                                                tableInfo.getTableRef(),
                                                columnInfo.getSourceColumn(),
                                                key.get(columnInfo.getName())));
                            } else if (OracleType.Type.RAW.equals(columnInfo.getType())) {
                                row.put(
                                        columnInfo.getName(),
                                        OracleLogMinerValueConverter.hexStringToByteArray(
                                                key.get(columnInfo.getName()).toString()));
                            }
                        });

        submitRecord(ChangeType.DELETE, row, changeTime);
    }

    private void handleRow(String rowId, Map<String, Object> sourceRow) {
        upsert(sourceRow, syncTime);
    }

    protected void processRows(ResultSet rows) throws SQLException {
        long recordCount = 0;

        while (rows.next()) {
            oracleApi.acceptRow(tableInfo.getTableRef(), rows, tableInfo.getOracleColumns(), this::handleRow);
            ++recordCount;
        }

        //LOG.customerInfo(ProcessedRecordsEvent.of(tableInfo.getTableRef().toString(), recordCount));
    }

    protected void checkpoint() {
        outputHelper.checkpoint(tableState.getOracleState());
    }

    public void validateTable() throws Exception {
        dbObjectValidator.validate(tableInfo);
    }

    public TableRef getTableRef() {
        return tableInfo.getTableRef();
    }
}
