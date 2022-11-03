package com.example.oracle;

import com.example.core.TableRef;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 9:56 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleResyncHelper {
    public boolean tableNeedsResync(TableRef tableRef) {
        return false;
    }

    public void addToResyncSet(TableRef tableRef, String reason, Boolean logEventNow) {

    }

    public int countTablesToResync() {
        return 0;
    }

    public void warnAboutResync() {

    }
}