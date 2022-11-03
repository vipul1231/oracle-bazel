package com.example.oracle.tasks;

import com.example.core.task.Task;
import com.example.core.task.TaskType;

import java.time.Instant;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 9:42 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class InsufficientFlashbackStorage implements Task {
    public static final TaskType<InsufficientFlashbackStorage> TYPE =
            new TaskType<InsufficientFlashbackStorage>() {

                @Override
                public String type() {
                    return "insufficient_flashback_storage";
                }

                @Override
                public String title() {
                    return "Insufficient Flashback Storage";
                }

//                @Override
//                public PathName keyTail(InsufficientFlashbackStorage task) {
//                    return PathName.of();
//                }

                @Override
                public Class<InsufficientFlashbackStorage> instanceClass() {
                    return InsufficientFlashbackStorage.class;
                }

//                @Override
//                public String renderBody(List<InsufficientFlashbackStorage> tasks) {
//                    return Markdowns.markdown(
//                            Paths.get("/core_interfaces/resources/task/InsufficientFlashbackStorage.md.vm"));
//                }
            };

    @Override
    public TaskType getType() {
        return TYPE;
    }

    @Override
    public boolean isIncrementable() {
        return true;
    }

    @Override
    public boolean shouldSendNotification(Integer iterationCounter, Instant emailSent) {
        return iterationCounter > 1;
    }

    @Override
    public boolean shouldSendNotificationDefault() {
        return false;
    }

    @Override
    public boolean shouldRescheduleOnFirstEncounter() {
        return false;
    }
}