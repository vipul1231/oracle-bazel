package com.example.oracle.exceptions;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 10:10 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class ImportFailureException extends Exception {
    public ImportFailureException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}