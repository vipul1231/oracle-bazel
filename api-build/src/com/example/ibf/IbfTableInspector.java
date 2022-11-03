package com.example.ibf;

import com.example.core.TableRef;
import java.util.Optional;

public interface IbfTableInspector {
    /** Get the TableRef of the associated table */
    TableRef tableRef();

    /** Get the estimated row count of the associated table */
    Optional<Long> estimatedRowCount();
}
