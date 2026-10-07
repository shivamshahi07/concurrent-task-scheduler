package com.shivamshahi.scheduler.event;

@FunctionalInterface
public interface TaskLifecycleListener {
    void onEvent(TaskSchedulerEvent event);
}
