package com.example.oracle;

import com.example.core.TableRef;

import java.util.List;
import java.util.Map;

public interface OracleIncrementalUpdaterFactory {
    OracleAbstractIncrementalUpdater newIncrementalUpdater(Map<TableRef, List<OracleColumn>> selected);
}
