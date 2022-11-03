package com.example.db;

import com.example.core.TableRef;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Optional;

public class Key {
    public final TableRef table;
    public final String name;

    public enum Type {
        PRIMARY,
        UNIQUE,
        FOREIGN
    }

    public final Type type;
    public final int[] columnIndices;
    public final Optional<TableRef> referencedTable;
    public final int[] referencedColumnIndices;

    public enum ReferenceMatchType {
        FULL,
        PARTIAL,
        SIMPLE
    }

    public final ReferenceMatchType referenceMatchType;

    public Key(
            TableRef table,
            String name,
            String type,
            Array columnIndices,
            Optional<TableRef> referencedTable,
            Array referencedColumnIndices,
            String referenceMatchType) {
        this.table = table;
        this.name = name;
        switch (type) {
            case "PRIMARY":
                this.type = Type.PRIMARY;
                break;
            case "UNIQUE":
                this.type = Type.UNIQUE;
                break;
            case "FOREIGN":
                this.type = Type.FOREIGN;
                break;
            default:
                throw new RuntimeException("Invalid key type: " + type);
        }
        try {
            assert columnIndices != null;
            String[] indices = (String[]) columnIndices.getArray();
            this.columnIndices = new int[indices.length];
            for (int i = 0; i < indices.length; i++) this.columnIndices[i] = new Integer(indices[i]);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to get column indices array", e);
        }
        this.referencedTable = referencedTable;
        try {
            if (referencedColumnIndices == null) {
                this.referencedColumnIndices = null;
            } else {
                String[] referencedIndices = (String[]) referencedColumnIndices.getArray();
                this.referencedColumnIndices = new int[referencedIndices.length];
                for (int i = 0; i < referencedIndices.length; i++)
                    this.referencedColumnIndices[i] = new Integer(referencedIndices[i]);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable to get referenced column indices array", e);
        }
        if (referenceMatchType == null) this.referenceMatchType = null;
        else
            switch (referenceMatchType) {
                case "FULL":
                    this.referenceMatchType = ReferenceMatchType.FULL;
                    break;
                case "PARTIAL":
                    this.referenceMatchType = ReferenceMatchType.PARTIAL;
                    break;
                case "SIMPLE":
                    this.referenceMatchType = ReferenceMatchType.SIMPLE;
                    break;
                default:
                    throw new RuntimeException("Invalid reference match type: " + type);
            }
    }
}
