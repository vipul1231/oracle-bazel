package com.example.sql_server;

import com.example.core2.Output;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 3:43 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class MockOutput2<S> extends Output<SqlServerState> {
    public MockOutput2(SqlServerState state) {
        super();
    }

    public <State> MockOutput2(State midImportState) {

    }
}