package com.example.core.annotations;

import com.example.core.TableRef;

import java.time.temporal.TemporalAccessor;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:52 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public enum DataType {
    BigDecimal, Unknown, Double, Short, Int, Long, String, LocalDate, LocalDateTime, Time, Instant, Binary, Float, Json, Boolean;

    public java.lang.String format(String s, TableRef tableRef, String rowId) {
        return null;
    }

    public java.lang.String format(String s, TableRef tableRef) {
        return null;
    }

    public java.lang.String format(String s, TableRef tableRef, Long tableStartScn, long currentScn) {
        return null;
    }

    public <T> T from(TemporalAccessor temporalAccessor) {
        return null;
    }
}