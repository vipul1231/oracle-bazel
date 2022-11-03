package com.example.ibf.db_compare;

public class IbfCompareColumn {
    private final IbfCompareDb db;
    private final IbfCompareDb representingDb;
    private final String sourceType;
    private final String name;

    public IbfCompareColumn(IbfCompareDb db, String sourceType, String name) {
        this(db, null, sourceType, name);
    }

    public IbfCompareColumn(
            IbfCompareDb db, IbfCompareDb representingDb, String sourceType, String name) {
        this.db = db;
        this.representingDb = representingDb;
        this.sourceType = sourceType;
        this.name = name;
    }

    public IbfCompareDb getDb() {
        return db;
    }

    public String getName() {
        return name;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getFullType() {
        // If the SqliteType represents a different source's type, then that source's name is already prefixed in the
        // sourceType.
        if (representingDb != null) return sourceType.toLowerCase();

        return db.name().toLowerCase() + "_" + sourceType.toLowerCase();
    }
}
