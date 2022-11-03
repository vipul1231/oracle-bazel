package com.example.oracle.tasks;

import com.example.core.task.Task;
import com.example.core.task.TaskType;

import java.time.Instant;

import static com.example.oracle.tasks.InsufficientFlashbackStorage.TYPE;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:30 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class UncommittedTransactionBlockingSync implements Task {
    public long scn;
    public int status;
    public int operationCode;
    public String segOwner;
    public String tableName;
    public String xid;
    public String rowId;

    public UncommittedTransactionBlockingSync(
            long scn, int status, int operationCode, String segOwner, String tableName, String xid, String rowId) {
        this.scn = scn;
        this.status = status;
        this.operationCode = operationCode;
        this.segOwner = segOwner;
        this.tableName = tableName;
        this.xid = xid;
        this.rowId = rowId;
    }

//    public static final SourceTaskType<UncommittedTransactionBlockingSync, OracleUpdater> TYPE =
//            new SourceTaskType<UncommittedTransactionBlockingSync, OracleUpdater>() {
//                @Override
//                public String type() {
//                    return "uncommitted_transaction_blocking_sync";
//                }
//
//                @Override
//                public String title() {
//                    return "Uncommitted Transaction Blocking Sync";
//                }
//
//                @Override
//                public PathName keyTail(UncommittedTransactionBlockingSync task) {
//                    return PathName.of(task.xid);
//                }
//
//                @Override
//                public Class<UncommittedTransactionBlockingSync> instanceClass() {
//                    return UncommittedTransactionBlockingSync.class;
//                }
//
//                @Override
//                public String renderBody(List<UncommittedTransactionBlockingSync> tasks) {
//                    try (Formatter formatter = new Formatter()) {
//                        formatter.format(
//                                "There is an uncommitted transaction that has been open for more than six hours. Please "
//                                        + "commit the transaction, rollback the transaction, or perform a full re-sync.\n");
//                        tasks.forEach(
//                                t ->
//                                        formatter.format(
//                                                "xid=%s rowId='%s' scn='%d' tableName='%s' segOwner='%s' status='%d' operationCode='%d'\n",
//                                                t.xid,
//                                                t.rowId,
//                                                t.scn,
//                                                t.tableName,
//                                                t.segOwner,
//                                                t.status,
//                                                t.operationCode));
//                        return formatter.toString();
//                    }
//                }
//            };

    public UncommittedTransactionBlockingSync() {}

    @Override
    public TaskType getType() {
        return TYPE;
    }

    @Override
    public boolean isIncrementable() {
        return false;
    }

    @Override
    public boolean shouldSendNotification(Integer iterationCounter, Instant emailSent) {
        return false;
    }

    @Override
    public boolean shouldSendNotificationDefault() {
        return false;
    }

    @Override
    public boolean shouldRescheduleOnFirstEncounter() {
        return false;
    }
}