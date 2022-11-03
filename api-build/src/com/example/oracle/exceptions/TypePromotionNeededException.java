package com.example.oracle.exceptions;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:56 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class TypePromotionNeededException extends Exception {
    public TypePromotionNeededException(String errorMessage) {
        super(errorMessage);
    }
}