package com.example.oracle;

import java.util.Objects;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 3:47 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class OracleDynamicPageSizeConfig {
    public final long start;
    public final long min;
    public final long step;

    public OracleDynamicPageSizeConfig(long start, long min, long step) {
        this.start = start;
        this.min = min;
        this.step = step;
    }

    public OracleDynamicPageSizeConfig() {
        start = 50_000L;
        min = 1_000L;
        step = 1_000L;
    }

    public long getMin() {
        return min;
    }

    public long getTempExtraStep() {
        return step * 2;
    }

    public long getStart() {
        return start;
    }

    public long getStep() {
        return step;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OracleDynamicPageSizeConfig that = (OracleDynamicPageSizeConfig) o;
        return start == that.start && min == that.min && step == that.step;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, min, step);
    }

    @Override
    public String toString() {
        return "OracleDynamicPageSizeConfig{" + "start=" + start + ", min=" + min + ", step=" + step + '}';
    }
}