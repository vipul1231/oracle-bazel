package com.example.core;

import com.example.db.TunnelableConnectionException;
import com.example.oracle.Rescheduled;
import com.example.oracle.exceptions.InvalidScnException;
import com.example.oracle.tasks.InsufficientFlashbackStorage;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:28 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class CompleteWithTask extends Rescheduled {
    public static RuntimeException create(InsufficientFlashbackStorage insufficientFlashbackStorage, Exception t) {
        return null;
    }

    public static InvalidScnException reconnect(Exception e, String fell_behind_archive_log) {
        return null;
    }

    public static Exception reconnect(TunnelableConnectionException e) {
        return null;
    }
}