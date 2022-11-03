package com.example.sql_server;

import com.example.core.StandardConfig;
import com.example.oracle.DataUpdater;
import com.example.oracle.RecordProcessor;
import com.example.oracle.Service;

/** This is a temporary workaround until Pipeline#clearTasks no longer needs a {@link DataUpdater}. */
@Deprecated
public class SqlServerUpdater implements DataUpdater<SqlServerState> {

    public final SqlServerState state;

    SqlServerUpdater(SqlServerState state) {
        this.state = state;
    }

//    @Override
    public Service<?, SqlServerState> service() {
        throw new UnsupportedOperationException();
    }

//    @Override
    public void update(RecordProcessor<SqlServerState> records, StandardConfig standardConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {

    }
}
