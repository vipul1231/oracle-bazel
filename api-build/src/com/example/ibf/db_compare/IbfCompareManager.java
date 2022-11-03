package com.example.ibf.db_compare;

import com.example.ibf.IBFDecodeResult;
import com.example.ibf.IbfSyncResult;
import com.example.ibf.InvertibleBloomFilter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class IbfCompareManager {
    private final IbfCompareTableSource source;
    private final IbfCompareTableDestination destination;
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public IbfCompareManager(IbfCompareTableSource source, IbfCompareTableDestination destination) {
        this.source = source;
        this.destination = destination;
    }

    public IbfSyncResult compare() throws SQLException {
        List<Callable<InvertibleBloomFilter>> todo = new ArrayList<>(2);
//        todo.add(() -> source.fetchCommonIbf(200));
//        todo.add(() -> destination.fetchCommonIbf(200, source.getSourceColumns()));

        IBFDecodeResult decodeResult = null;
        try {
            List<Future<InvertibleBloomFilter>> futures = executorService.invokeAll(todo);

            decodeResult = futures.get(0).get().compare(futures.get(1).get());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                SQLException throwable = (SQLException) e.getCause();
                throw throwable;
            } else {
                // We should never get here
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            // TODO: revisit handling (as if decode failed)
            throw new RuntimeException(e);
        }

        return new IbfSyncResult(decodeResult, source.keyTypes(), source.keyLengths());
    }
}
