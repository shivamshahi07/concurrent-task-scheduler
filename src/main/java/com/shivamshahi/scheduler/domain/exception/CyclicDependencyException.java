package com.shivamshahi.scheduler.domain.exception;

public class CyclicDependencyException extends SchedulerException {
    public CyclicDependencyException(String message) {
        super(message);
    }
}
