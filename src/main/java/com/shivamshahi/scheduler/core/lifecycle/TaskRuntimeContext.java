package com.shivamshahi.scheduler.core.lifecycle;

import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.status.JobStatus;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe runtime tracking for an active or scheduled task.
 */
public class TaskRuntimeContext {
    private final JobId jobId;
    private final Instant submittedAt;
    private volatile JobStatus status;
    private final AtomicInteger attemptCount;
    private volatile Instant executionScheduledTime;
    private volatile Instant lastExecutionStartTime;
    private volatile Object lastResult;
    private volatile Throwable lastFailure;
    private volatile Thread executingThread;

    public TaskRuntimeContext(JobId jobId, Instant executionScheduledTime) {
        this.jobId = Objects.requireNonNull(jobId, "JobId cannot be null");
        this.submittedAt = Instant.now();
        this.status = JobStatus.QUEUED;
        this.attemptCount = new AtomicInteger(0);
        this.executionScheduledTime = executionScheduledTime;
    }

    public synchronized boolean tryTransitionTo(JobStatus newStatus) {
        if (this.status.canTransitionTo(newStatus)) {
            this.status = newStatus;
            return true;
        }
        return false;
    }

    public JobId getJobId() {
        return jobId;
    }

    public Instant getSubmittedAt() {
        return submittedAt;
    }

    public JobStatus getStatus() {
        return status;
    }

    public int incrementAndGetAttempts() {
        return attemptCount.incrementAndGet();
    }

    public int getAttemptCount() {
        return attemptCount.get();
    }

    public Instant getExecutionScheduledTime() {
        return executionScheduledTime;
    }

    public void setExecutionScheduledTime(Instant executionScheduledTime) {
        this.executionScheduledTime = executionScheduledTime;
    }

    public Instant getLastExecutionStartTime() {
        return lastExecutionStartTime;
    }

    public void setLastExecutionStartTime(Instant lastExecutionStartTime) {
        this.lastExecutionStartTime = lastExecutionStartTime;
    }

    public Object getLastResult() {
        return lastResult;
    }

    public void setLastResult(Object lastResult) {
        this.lastResult = lastResult;
    }

    public Throwable getLastFailure() {
        return lastFailure;
    }

    public void setLastFailure(Throwable lastFailure) {
        this.lastFailure = lastFailure;
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    public void setExecutingThread(Thread executingThread) {
        this.executingThread = executingThread;
    }
}
