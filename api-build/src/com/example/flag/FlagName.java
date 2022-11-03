package com.example.flag;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:41 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class FlagName {

    public static Object SqlServerParallelizedUpdates;
    public static Object BridgeDecoupledCore;
    public static MysqlImportWithoutHexingBinaryPKCol SQLServerHistoryModeCorrection;

    public static class OracleLogMinerNoContinuousMine {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleProcessUncommittedTransactionsCommittedLater {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleSkipIncrementalWhenNothingToImport {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleExplicitTypeForNumbersAndDates {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleFollowNewSubmitRecordCodePath {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleFilterNonTableTypesFromPermissionCheck {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleReplaceInvaldNumberWithNULL {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleUseOptimizedVersionQuery {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleSortVersionsOperation {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleSkipIncrementalSyncBeforeImportStarts {
        public static boolean check() {
            return false;
        }
    }

    public static class SqlServerOrderByPK {
        public static boolean check() {
            return false;
        }
    }

    public class SqlServerParallelizedUpdates {
    }

    public class BridgeDecoupledCore {
    }

    public static class MysqlImportWithoutHexingBinaryPKCol {
        public static boolean check() {
            return true;
        }
    }

    public static class SqlServerAggregateMinVersionQuery {
        public static boolean check() {
            return false;
        }
    }

    public static class SqlServerTrident {
        public static boolean check() {
            return false;
        }
    }

    public static class MysqlTrident {
        public static boolean check() {
            return false;
        }
    }

    public static class RunIncrementalSyncOnlyOnImportedTables {
        public static boolean check() {
            return false;
        }
    }

    public static class ReduceTableSchemaAssertions {
        public static boolean check() {
            return false;
        }
    }

    public static class SqlServerHistoryModeUtcOpTime {
        public static boolean check() {
            return false;
        }
    }

    public static class AllowedList {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleFixHistoryModeTimeStamp {
        public static boolean check() {
            return false;
        }
    }

    public static class OracleSaveUncommittedTransactionsToJailTable {

        /**
         * @TODO: Need to write business logic
         * @return
         */
        public static boolean check() {
            return Boolean.TRUE;
        }
    }

    /**
     * @TODO: Need to write business logic
     */
    public static class OracleProcessRollbackInserts {
        public static boolean check() {
            return Boolean.TRUE;
        }
    }

    public static class OracleFixPKlessTableWithRowMovement {
        /**
         * @TODO: Need to write business logic
         * @return
         */
        public static boolean check() {
            return Boolean.TRUE;
        }
    }
}
