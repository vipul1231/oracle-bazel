package com.example.oracle.spi;

import com.example.core.TableRef;
import com.example.oracle.OracleColumn;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 11:46 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public interface OracleIncrementalUpdaterFactory {
    OracleAbstractIncrementalUpdater newIncrementalUpdater(Map<TableRef, List<OracleColumn>> selected);
}
