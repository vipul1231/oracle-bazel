package com.example.core.task;

import com.example.oracle.tasks.InsufficientFlashbackStorage;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:42 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public abstract class TaskType<I> {
    public abstract String type();

    public abstract String title();

    public abstract Class<InsufficientFlashbackStorage> instanceClass();
}