package com.example.oracle.exceptions;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 1:56 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class MissingObjectException extends RuntimeException {
    public MissingObjectException(String errorMessage) {
        super(errorMessage);
    }
}