package com.example.snowflakecritic.ibf;

import com.example.core.Column;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.ibf.db_incremental_sync.IbfTableEncoder;
import com.example.snowflakecritic.SnowflakeTableInfo;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SnowflakeIbfBase {
    protected static final String TEMPLATE_FILENAME = "snowflake_ibf.sql.vm";
    protected final DataSource source;
    protected final List<Column> primaryKeyColumns;
    protected SnowflakeTableInfo tableInfo;

    public SnowflakeIbfBase(DataSource source, SnowflakeTableInfo tableInfo) {
        this.source = source;
        this.tableInfo = tableInfo;
        this.primaryKeyColumns =
                tableInfo.sourceColumns().stream().filter(c -> c.primaryKey).collect(Collectors.toList());
    }

    public TableRef tableRef() {
        return tableInfo.sourceTable;
    }

    public Optional<Long> estimatedRowCount() {
        return Optional.empty();
    }

    public List<Integer> keyLengths() {
        return primaryKeyColumns.stream().map(this::keyLength).collect(Collectors.toList());
    }

    public List<DataType> keyTypes() {
        return primaryKeyColumns.stream().map(c -> c.type).collect(Collectors.toList());
    }

    private int keyLength(Column primaryKey) {
        if (primaryKey.type == DataType.String)
            return IbfDbUtils.computeKeyLength(primaryKey.byteLength.orElse(0));
        return IbfTableEncoder.DEFAULT_KEY_LENGTH;
    }

    public List<String> getKeyNames() {
        return primaryKeyColumns.stream().map(c -> c.name).collect(Collectors.toList());
    }

    public class TemplateHelper {
        public String quote(String entity) {
            return '"' + entity.replace("\"", "\"\"") + '"';
        }

//        public boolean isNumber(DataType keyType) {
//            return keyType.csvType() == CsvSchema.ColumnType.NUMBER;
//        }
//
//        public boolean isString(DataType keyType) {
//            return keyType.csvType() == CsvSchema.ColumnType.STRING;
//        }
    }
}
