package com.shivamshahi.scheduler.event;

import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.status.JobStatus;

import java.time.Duration;
import java.time.Instant;

public interface TaskSchedulerEvent {
    JobId getJobId();
    Instant getOccurredAt();
    String getEventType();

    record TaskSubmitted(JobId jobId, String taskName, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "TASK_SUBMITTED"; }
    }

    record TaskStarted(JobId jobId, String workerThreadName, int attemptNumber, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "TASK_STARTED"; }
    }

    record TaskCompleted(JobId jobId, Duration duration, Object result, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "TASK_COMPLETED"; }
    }

    record TaskFailed(JobId jobId, int attemptNumber, Throwable error, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "TASK_FAILED"; }
    }

    record RetryScheduled(JobId jobId, int nextAttempt, Duration delay, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "RETRY_SCHEDULED"; }
    }

    record TaskCancelled(JobId jobId, JobStatus previousStatus, Instant occurredAt) implements TaskSchedulerEvent {
        @Override public JobId getJobId() { return jobId; }
        @Override public Instant getOccurredAt() { return occurredAt; }
        @Override public String getEventType() { return "TASK_CANCELLED"; }
    }
}
