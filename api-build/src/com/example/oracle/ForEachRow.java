package com.example.oracle;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 11:48 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
@FunctionalInterface
interface ForEachRow {
    void accept(String rowId, Map<String, Object> row);
}