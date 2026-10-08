package com.shivamshahi.scheduler.core;

import com.shivamshahi.scheduler.api.TaskScheduler;
import com.shivamshahi.scheduler.core.lifecycle.TaskRuntimeContext;
import com.shivamshahi.scheduler.core.queue.PrioritizedTaskItem;
import com.shivamshahi.scheduler.core.queue.TaskDispatchQueue;
import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.priority.Priority;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.event.EventDispatcher;
import com.shivamshahi.scheduler.event.TaskLifecycleListener;
import com.shivamshahi.scheduler.event.TaskSchedulerEvent;
import com.shivamshahi.scheduler.metrics.SchedulerMetrics;
import com.shivamshahi.scheduler.policy.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CoreComponentsTest {

    @Test
    @DisplayName("Test TaskDispatchQueue poll, offer, remove, and size")
    void testDispatchQueue() throws InterruptedException {
        TaskDispatchQueue queue = new TaskDispatchQueue(5);
        assertThat(queue.isEmpty()).isTrue();

        TaskDefinition task1 = TaskDefinition.newBuilder().name("task1").priority(Priority.LOW).action(() -> {}).build();
        TaskDefinition task2 = TaskDefinition.newBuilder().name("task2").priority(Priority.CRITICAL).action(() -> {}).build();

        queue.offer(task1);
        queue.offer(task2);
        assertThat(queue.size()).isEqualTo(2);

        PrioritizedTaskItem first = queue.poll();
        assertThat(first.getTaskDefinition().getPriority()).isEqualTo(Priority.CRITICAL);

        PrioritizedTaskItem timed = queue.poll(10, TimeUnit.MILLISECONDS);
        assertThat(timed.getTaskDefinition().getPriority()).isEqualTo(Priority.LOW);

        assertThat(queue.poll()).isNull();

        queue.offer(task1);
        assertThat(queue.remove(task1.getJobId())).isTrue();
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Test TaskRuntimeContext accessors and mutators")
    void testRuntimeContext() {
        JobId jobId = JobId.generate();
        Instant now = Instant.now();
        TaskRuntimeContext ctx = new TaskRuntimeContext(jobId, now);

        assertThat(ctx.getJobId()).isEqualTo(jobId);
        assertThat(ctx.getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(ctx.getSubmittedAt()).isNotNull();

        ctx.setExecutionScheduledTime(now.plusSeconds(5));
        assertThat(ctx.getExecutionScheduledTime()).isEqualTo(now.plusSeconds(5));

        ctx.setLastResult("Sample Result");
        assertThat(ctx.getLastResult()).isEqualTo("Sample Result");

        Exception err = new RuntimeException("Test err");
        ctx.setLastFailure(err);
        assertThat(ctx.getLastFailure()).isEqualTo(err);
    }

    @Test
    @DisplayName("Test SchedulerMetrics counters")
    void testMetrics() {
        SchedulerMetrics metrics = new SchedulerMetrics();
        metrics.incrementSubmitted();
        metrics.incrementCompleted();
        metrics.incrementFailed();
        metrics.incrementRetry();
        metrics.incrementCancelled();

        assertThat(metrics.getSubmittedCount()).isEqualTo(1);
        assertThat(metrics.getCompletedCount()).isEqualTo(1);
        assertThat(metrics.getFailedCount()).isEqualTo(1);
        assertThat(metrics.getRetryCount()).isEqualTo(1);
        assertThat(metrics.getCancelledCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Test TaskSchedulerEvent record getters and event types")
    void testEvents() {
        JobId jobId = JobId.generate();
        Instant now = Instant.now();

        TaskSchedulerEvent.TaskSubmitted sub = new TaskSchedulerEvent.TaskSubmitted(jobId, "taskName", now);
        assertThat(sub.getEventType()).isEqualTo("TASK_SUBMITTED");
        assertThat(sub.getJobId()).isEqualTo(jobId);
        assertThat(sub.getOccurredAt()).isEqualTo(now);

        TaskSchedulerEvent.TaskStarted started = new TaskSchedulerEvent.TaskStarted(jobId, "worker-1", 1, now);
        assertThat(started.getEventType()).isEqualTo("TASK_STARTED");

        TaskSchedulerEvent.TaskCompleted comp = new TaskSchedulerEvent.TaskCompleted(jobId, Duration.ofMillis(50), "OK", now);
        assertThat(comp.getEventType()).isEqualTo("TASK_COMPLETED");

        TaskSchedulerEvent.TaskFailed fail = new TaskSchedulerEvent.TaskFailed(jobId, 1, new RuntimeException("boom"), now);
        assertThat(fail.getEventType()).isEqualTo("TASK_FAILED");

        TaskSchedulerEvent.RetryScheduled retry = new TaskSchedulerEvent.RetryScheduled(jobId, 2, Duration.ofMillis(100), now);
        assertThat(retry.getEventType()).isEqualTo("RETRY_SCHEDULED");

        TaskSchedulerEvent.TaskCancelled cancel = new TaskSchedulerEvent.TaskCancelled(jobId, JobStatus.QUEUED, now);
        assertThat(cancel.getEventType()).isEqualTo("TASK_CANCELLED");
    }

    @Test
    @DisplayName("Test EventDispatcher listener addition, removal, and fault tolerance")
    void testEventDispatcher() {
        EventDispatcher dispatcher = new EventDispatcher();
        TaskLifecycleListener faultListener = event -> { throw new RuntimeException("Subscriber error"); };
        dispatcher.addListener(faultListener);

        JobId id = JobId.generate();
        // Should not throw even when listener throws
        dispatcher.dispatch(new TaskSchedulerEvent.TaskSubmitted(id, "task", Instant.now()));

        dispatcher.removeListener(faultListener);
    }

    @Test
    @DisplayName("Test RetryPolicy default noRetries")
    void testDefaultNoRetries() {
        RetryPolicy policy = RetryPolicy.noRetries();
        assertThat(policy.getMaxAttempts()).isEqualTo(1);
        assertThat(policy.shouldRetry(1, new RuntimeException())).isFalse();
        assertThat(policy.getBackoffDuration(1)).isEqualTo(Duration.ZERO);
    }
}
