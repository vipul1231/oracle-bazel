package com.example.oracle;

import com.example.core.Column;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.flag.FlagName;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:16 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

public class OracleColumn {
    public final String name;
    DataType warehouseType;
    DataType jdbcExtractionType;
    final OracleType oracleType;
    public final boolean primaryKey;
    final TableRef sourceTable;
    final Optional<TableRef> foreignKey;
    Set<String> badDateVals;
    /** The value from all_tab_columns.data_default. Can represent a literal value or a PL/SQL expression. */
    private Optional<String> dataDefault = Optional.empty();

    OracleColumn(
            String name,
            OracleType oracleType,
            boolean primaryKey,
            TableRef sourceTable,
            Optional<TableRef> foreignKey) {
        assert oracleType.getWarehouseType().isPresent();

        this.name = name;
//        this.warehouseType = oracleType.getWarehouseType().get();
        if (FlagName.OracleExplicitTypeForNumbersAndDates.check()) {
            this.jdbcExtractionType = warehouseType;
        } else {
//            this.jdbcExtractionType = oracleType.getExtractionType().orElse(warehouseType);
        }
        this.oracleType = oracleType;
        this.primaryKey = primaryKey;
        this.sourceTable = sourceTable;
        this.foreignKey = foreignKey;
        this.badDateVals = new HashSet<>();
    }

    OracleColumn withPrimaryKey() {
        return new OracleColumn(name, oracleType, true, sourceTable, foreignKey);
    }

    OracleColumn withForeignKey(Optional<TableRef> foreignKey) {
        return new OracleColumn(name, oracleType, primaryKey, sourceTable, foreignKey);
    }

    Column asexampleColumn(String inSchema) {
        Optional<String> sameSchemaForeignKey =
                foreignKey.filter(candidate -> candidate.schema.equals(inSchema)).map(candidate -> candidate.name);

//        return new Column.Builder(name, warehouseType)
//                .primaryKey(primaryKey)
//                .foreignKey(sameSchemaForeignKey)
//                .byteLength(oracleType.getColumnByteLength())
//                .precision(oracleType.getColumnPrecision())
//                .scale(oracleType.getColumnScale())
//                .build();

        return null;
    }

    void updateWarehouseType(DataType newType) {
        this.warehouseType = newType;
        this.jdbcExtractionType = newType;
    }

    /**
     * Use this method to determine if a column has a default value. If the return values is present you then need to
     * determine if it represents a literal value or an expression. If it is a literal value then you should be able to
     * convert that to an appropriate default column value. If it is an expression then the column has a 'dynamic'
     * default value.
     *
     * @return optional data_default value
     */
    public Optional<String> getDataDefault() {
        return dataDefault;
    }

    public TableRef getSourceTable() {
        return sourceTable;
    }

    public DataType getWarehouseType() {
        return warehouseType;
    }

}