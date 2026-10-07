package com.shivamshahi.scheduler.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic scheduler execution statistics and metrics.
 */
public class SchedulerMetrics {
    private final AtomicLong submittedCount = new AtomicLong(0);
    private final AtomicLong completedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    private final AtomicLong cancelledCount = new AtomicLong(0);

    public void incrementSubmitted() { submittedCount.incrementAndGet(); }
    public void incrementCompleted() { completedCount.incrementAndGet(); }
    public void incrementFailed() { failedCount.incrementAndGet(); }
    public void incrementRetry() { retryCount.incrementAndGet(); }
    public void incrementCancelled() { cancelledCount.incrementAndGet(); }

    public long getSubmittedCount() { return submittedCount.get(); }
    public long getCompletedCount() { return completedCount.get(); }
    public long getFailedCount() { return failedCount.get(); }
    public long getRetryCount() { return retryCount.get(); }
    public long getCancelledCount() { return cancelledCount.get(); }
}
