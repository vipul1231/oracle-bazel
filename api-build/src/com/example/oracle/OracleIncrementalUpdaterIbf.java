package com.example.oracle;

import com.example.core.TableRef;
import com.example.logger.ExampleLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OracleIncrementalUpdaterIbf extends OracleAbstractIncrementalUpdater{
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private Collection<IbfTableWorker> tableWorkers;

    private boolean importPhaseComplete;

    public OracleIncrementalUpdaterIbf(
            OracleState state,
            OracleOutputHelper outputHelper,
            OracleResyncHelper resyncHelper,
            OracleMetricsHelper metricsHelper,
            Map<TableRef, List<OracleColumn>> selectedTables,
            Collection<IbfTableWorker> tableWorkers) {
        super(state, outputHelper, resyncHelper, metricsHelper, selectedTables);
        this.tableWorkers = tableWorkers;
    }

    @Override
    public void start() {
        LOG.info("Ibf: creating baseline IBFs");

        Collection<IbfTableWorker> invalidTableWorkers = new ArrayList<>();

        // This must occur before initial sync of tables
        // Each table worker will determine if it has pre-initial sync work to do or not.
        for (IbfTableWorker tableWorker : tableWorkers) {
            try {
                tableWorker.validateTable();
                tableWorker.performPreImport();
            } catch (Exception ex) {
                //LOG.warning("Cannot import table " + tableWorker.getTableRef(), ex);
                invalidTableWorkers.add(tableWorker);
            }
        }

        if (!invalidTableWorkers.isEmpty()) {
            tableWorkers.removeAll(invalidTableWorkers);
        }
    }

    @Override
    public void stop() {
        importPhaseComplete = true;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void doIncrementalWork() {
        // Don't do anything while import is running. Wait until all tables have been imported
        if (!importPhaseComplete) {
            return;
        }

        LOG.info("Ibf: performing incremental updates");

        tableWorkers.forEach(
                worker -> {
                    worker.performIncrementalSync();
//                    try (OracleMetricsHelper.LongTaskTimerSampler ignored =
//                                 metricsHelper.getExtractPhaseTimer().start()) {
//                        worker.performIncrementalSync();
//                    }
                    doCheckpoint();
                });
    }
}
