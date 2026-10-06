package com.shivamshahi.scheduler.domain.exception;

public class SchedulerException extends RuntimeException {
    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }
}
