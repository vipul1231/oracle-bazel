package com.example.oracle.warnings;

import com.example.core.PathName;

import java.util.List;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 7:38 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public abstract class WarningType<T> {
    public abstract String title();

    public abstract String type();

    public abstract PathName keyTail(NoTableAccessWarning warning);

    public abstract Class<NoTableAccessWarning> instanceClass();

    public abstract String renderBody(List<NoTableAccessWarning> warnings);
}