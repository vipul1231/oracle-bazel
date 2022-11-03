package com.example.oracle.exceptions;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/7/2021<br/>
 * Time: 4:47 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class QuickBlockRangeFailedException extends Exception {
    public QuickBlockRangeFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public QuickBlockRangeFailedException(String message) {
        super(message);
    }
}