package com.example.oracle.exceptions;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:25 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class InvalidScnException extends RuntimeException {
    public InvalidScnException(String message) {
        super(message);
    }

    public InvalidScnException(String message, Exception cause) {
        super(message, cause);
    }
}