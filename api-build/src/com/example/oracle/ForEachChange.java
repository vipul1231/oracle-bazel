package com.example.oracle;

import com.example.core.TableRef;

import java.util.Map;
import java.util.Optional;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:17 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
@FunctionalInterface
public interface ForEachChange {
    void accept(
            TableRef table,
            String rowId,
            Map<String, Object> row,
            ChangeType type,
            Optional<Transactions.RowPrimaryKeys> rowPrimaryKeys);
}