package com.example.oracle;

import com.example.core.TableRef;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class OracleAbstractIncrementalUpdater implements AutoCloseable {
    // Unfortunately in order to reduce overall impact of moving this field it must be made public (for now)
    public final OracleState state;
    // Unfortunately in order to reduce overall impact of moving this field it must be made public (for now)
    public OracleOutputHelper outputHelper;
    protected final OracleResyncHelper resyncHelper;
    protected final OracleMetricsHelper metricsHelper;
    protected final Map<TableRef, List<OracleColumn>> includedTables;

    public OracleAbstractIncrementalUpdater(
            OracleState state,
            OracleOutputHelper outputHelper,
            OracleResyncHelper resyncHelper,
            OracleMetricsHelper metricsHelper,
            Map<TableRef, List<OracleColumn>> includedTables) {
        this.state = state;
        this.outputHelper = outputHelper;
        this.resyncHelper = resyncHelper;
        this.metricsHelper = metricsHelper;
        this.includedTables = ImmutableMap.copyOf(includedTables);
    }

    /** Called once prior to any incremental syncing. Use this for starting threads, for example. */
    public void start() {
        // Nothing to do by default.
    }

    public void resync(TableRef tableRef, String reason, Boolean logEventNow) {
        resyncHelper.addToResyncSet(tableRef, reason, logEventNow);
        state.resetTable(tableRef);
    }

    public void forEachChange(
            TableRef table,
            String rowId,
            Map<String, Object> row,
            ChangeType type,
            Instant timeOfChange,
            Optional<Transactions.RowPrimaryKeys> maybeRowPrimaryKeys,
            Map<TableRef, List<OracleColumn>> selected) {
        throw new UnsupportedOperationException();
    }

    /** Called after all incremental syncing (extraction) has been completed */
    public void stop() {
        // Nothing to do
    }

    @Override
    public abstract void close();

    /** Perform some incremental updates */
    public abstract void doIncrementalWork();

    protected void doCheckpoint() {
        outputHelper.checkpoint(state);
    }

    protected Set<TableRef> getIncludedTables() {
        return includedTables.keySet();
    }
}
