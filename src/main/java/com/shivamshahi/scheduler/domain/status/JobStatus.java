package com.shivamshahi.scheduler.domain.status;

/**
 * Task execution lifecycle state machine.
 * Allowed transitions:
 * QUEUED -> RUNNING
 * QUEUED -> CANCELLED
 * QUEUED -> FAILED
 * RUNNING -> COMPLETED
 * RUNNING -> FAILED
 * RUNNING -> RETRY_WAITING
 * RUNNING -> QUEUED
 * RUNNING -> CANCELLED
 * RETRY_WAITING -> QUEUED
 * RETRY_WAITING -> CANCELLED
 * RETRY_WAITING -> FAILED
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    RETRY_WAITING,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }

    public boolean canTransitionTo(JobStatus next) {
        if (this == next) return false;
        return switch (this) {
            case QUEUED -> next == RUNNING || next == CANCELLED || next == FAILED;
            case RUNNING -> next == COMPLETED || next == FAILED || next == RETRY_WAITING || next == QUEUED || next == CANCELLED;
            case RETRY_WAITING -> next == QUEUED || next == CANCELLED || next == FAILED;
            case COMPLETED, FAILED, CANCELLED -> false;
        };
    }
}
