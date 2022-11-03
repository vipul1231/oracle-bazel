package com.example.oracle;

import com.example.core.warning.Warning;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 12:34 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
@FunctionalInterface
public interface WarningIssuer {
    void issueWarning(Warning warning);
}