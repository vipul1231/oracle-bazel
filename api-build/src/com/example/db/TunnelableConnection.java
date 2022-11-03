package com.example.db;

import com.example.core.DbCredentials;
import com.example.core2.ConnectionParameters;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 3:15 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class TunnelableConnection {
    public static boolean usePortForwarder;
    public String host;
    public int port;

    public <SqlCreds extends DbCredentials> TunnelableConnection(SqlCreds originalCredentials, ConnectionParameters params, boolean b) {
        this.host = originalCredentials.host;
        this.port = originalCredentials.port;
    }
}