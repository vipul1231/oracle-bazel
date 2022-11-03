package com.example.oracle.warnings;

import com.example.core.PathName;

import java.util.List;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:37 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class UncommittedWarning {

    private static final String UNKNOWN = "unknown";

    public static final WarningType<UncommittedWarning> TYPE =
            new WarningType<UncommittedWarning>() {
                @Override
                public String title() {
                    return "Uncommitted transactions";
                }

                @Override
                public String type() {
                    return "uncommitted_transactions";
                }

                @Override
                public PathName keyTail(NoTableAccessWarning warning) {
                    return null;
                }

                @Override
                public Class<NoTableAccessWarning> instanceClass() {
                    return null;
                }

//                @Override
//                public PathName keyTail(UncommittedWarning uncommittedWarning) {
//                    return PathName.of(String.valueOf(uncommittedWarning.scn), uncommittedWarning.xid);
//                }

//                @Override
//                public Class<UncommittedWarning> instanceClass() {
//                    return UncommittedWarning.class;
//                }

                @Override
                public String renderBody(List<NoTableAccessWarning> warnings) {
//                    List<String> warningStrings =
//                            warnings.stream().map(UncommittedWarning::toString).collect(Collectors.toList());
//                    return Markdowns.markdownVelocity(
//                            Paths.get("/integrations/oracle/resources/warnings/UncommittedWarning.md.vm"),
//                            ImmutableMap.of("errors", warningStrings));
                    return null;
                }
            };

    public long scn;
    public int status;
    public int operationCode;
    public String segOwner = UNKNOWN;
    public String tableName = UNKNOWN;
    public String xid = UNKNOWN;
    public String rowId = UNKNOWN;

    public UncommittedWarning() {}

    public UncommittedWarning(
            long scn, int status, int operationCode, String segOwner, String tableName, String xid, String rowId) {
        this.scn = scn;
        this.status = status;
        this.operationCode = operationCode;
        this.segOwner = segOwner;
        this.tableName = tableName;
        this.xid = xid;
        this.rowId = rowId;
    }

//    @Override
//    public WarningType getType() {
//        return TYPE;
//    }

    @Override
    public String toString() {
        return "{"
                + "xid='"
                + xid
                + '\''
                + ", rowId='"
                + rowId
                + '\''
                + ", scn="
                + scn
                + ", tableName='"
                + tableName
                + '\''
                + ", segOwner='"
                + segOwner
                + '\''
                + ", status='"
                + status
                + '\''
                + ", operationCode="
                + operationCode
                + '}';
    }
}