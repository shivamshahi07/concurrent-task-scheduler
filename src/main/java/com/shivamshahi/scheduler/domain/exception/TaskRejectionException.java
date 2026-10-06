package com.shivamshahi.scheduler.domain.exception;

public class TaskRejectionException extends SchedulerException {
    public TaskRejectionException(String message) {
        super(message);
    }
}
