package com.example.core.task;

import java.time.Instant;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 4:31 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public interface Task {
    TaskType getType();

    boolean isIncrementable();

    boolean shouldSendNotification(Integer iterationCounter, Instant emailSent);

    boolean shouldSendNotificationDefault();

    boolean shouldRescheduleOnFirstEncounter();
}