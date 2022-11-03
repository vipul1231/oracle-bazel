package com.example.sql_server;

import com.example.core.TableRef;
import com.example.db.DbRow;
import com.example.db.DbRowValue;

public class RowHelper {
    public DbRow<DbRowValue> simpleRow(int i, String foo) {
        return null;
    }

    public DbRowValue foreignKeyRefRowValue(String s, int i, TableRef parentTableA, String primaryKeyName) {
        return null;
    }

    public DbRow<DbRowValue> primaryKeylessRow(int i) {
        return null;
    }


//     private final long timestamp;
//     private final RowOuterClass.Op operation;
//
//     private RowHelper(RowOuterClass.Op operation, long startTs, long endTs) {
//     this.operation = operation;
//     this.timestamp = operation == RowOuterClass.Op.DELETE ? endTs : startTs;
//     }
//
//     private static RowHelper fromProto(RowOuterClass.Row row) {
//     return new RowHelper(row.getOp(), row.getExampleStart(), row.getExampleEnd());
//     }
//
//     private static RowHelper fromValue(Value row) {
//     return new RowHelper(row.operation, row.startTs, row.endTs);
//     }
//
//     static boolean areOperationTimestampsEqual( RowOuterClass.Row row1,  RowOuterClass.Row row2) {
//     return compareOperationTimestamp(fromProto(row1), fromProto(row2)) == 0;
//     }
//
//     static boolean areOperationTimestampsEqual( Value row1,  Value row2) {
//     return compareOperationTimestamp(fromValue(row1), fromValue(row2)) == 0;
//     }
//
//     static void validateLatestTimestampGreaterThanPrevious(
//      RowOuterClass.Row latestRow,  RowOuterClass.Row previousRow,  TableRef tableRef) {
//     throwIfIncorrectTimestamp(fromProto(latestRow), fromProto(previousRow), tableRef);
//     }
//
//     static void validateLatestTimestampGreaterThanPrevious(
//      Value latestRow,  Value previousRow, TableRef tableRef) {
//     throwIfIncorrectTimestamp(fromValue(latestRow), fromValue(previousRow), tableRef);
//     }
//
//     private static int compareOperationTimestamp( RowHelper latest,  RowHelper previous) {
//     return Long.compare(latest.timestamp, previous.timestamp);
//     }
//
//     private static void throwIfIncorrectTimestamp(
//      RowHelper latest,  RowHelper previous, TableRef tableRef) {
//     if (compareOperationTimestamp(latest, previous) < 0) {
//     throw new PipelineDataCorruptedException(
//     "Schema = "
//     + tableRef.schema
//     + "\n"
//     + "Table = "
//     + tableRef.name
//     + "\n"
//     + "Latest operation "
//     + latest.operation.name()
//     + " at timestamp "
//     + Instant.ofEpochMilli(latest.timestamp)
//     + " is less than"
//     + " previous operation "
//     + previous.operation.name()
//     + " at timestamp "
//     + Instant.ofEpochMilli(previous.timestamp)
//     + " for two records having the same primary key");
//     }
//     }


}
