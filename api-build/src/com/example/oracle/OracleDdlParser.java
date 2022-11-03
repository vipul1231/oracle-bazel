package com.example.oracle;

import com.example.logger.ExampleLogger;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:11 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleDdlParser {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();
    public final DdlType ddlType;
    private final String ddlStatement;

    public OracleDdlParser(String ddlStatement) {
        this.ddlStatement = ddlStatement.toUpperCase();
        this.ddlType = parseDdlType();
    }

    public Boolean shouldResync() {
        switch (ddlType) {
            case ALTER:
                return alterShouldResync();
            case ANALYZE:
            case ASSOCIATE_STATISTICS:
            case AUDIT:
            case COMMENT:
            case DISASSOCIATE_STATISTICS:
            case GRANT:
            case NOAUDIT:
            case PURGE:
            case REVOKE:
            case TRUNCATE:
                return false;
            case CREATE:
            case DROP:
            case FLASHBACK:
            case RENAME:
            default:
                return true;
        }
    }

    DdlType parseDdlType() {
        String[] words = ddlStatement.trim().split("\\s+");
        String ddlTypeString = words[0].trim();

        if (ddlTypeString.equals("ASSOCIATE") || ddlTypeString.equals("DISASSOCIATE")) {
            ddlTypeString += "_" + words[1].trim();
        }

        try {
            return DdlType.valueOf(ddlTypeString);
        } catch (IllegalArgumentException e) {
            LOG.warning("Unexpected DDL statement found: " + ddlStatement);
            return DdlType.UNDEFINED;
        }
    }

    Boolean alterShouldResync() {
        String cleanedStatement = ddlStatement.replaceAll("\\s+", " ");

        if (cleanedStatement.contains("SHRINK SPACE CHECK")) {
            return false;
        }

        if (cleanedStatement.contains("ENABLE NOVALIDATE CONSTRAINT")) {
            return false;
        }

        if (cleanedStatement.contains("ADD SUPPLEMENTAL LOG DATA")) {
            return false;
        }

        if (cleanedStatement.contains("FLASHBACK ARCHIVE")) {
            return false;
        }

        return true;
    }

    /**
     * @TODO: Need to add business logic
     * @return
     */
    public boolean isTruncate() {
        return Boolean.TRUE;
    }
}