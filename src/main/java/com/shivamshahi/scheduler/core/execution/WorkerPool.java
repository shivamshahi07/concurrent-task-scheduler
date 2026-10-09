package com.shivamshahi.scheduler.core.execution;

import com.shivamshahi.scheduler.core.dependency.DependencyGraph;
import com.shivamshahi.scheduler.core.lifecycle.TaskRuntimeContext;
import com.shivamshahi.scheduler.core.queue.PrioritizedTaskItem;
import com.shivamshahi.scheduler.core.queue.TaskDispatchQueue;
import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.event.EventDispatcher;
import com.shivamshahi.scheduler.event.TaskSchedulerEvent;
import com.shivamshahi.scheduler.metrics.SchedulerMetrics;
import com.shivamshahi.scheduler.policy.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker pool managing named worker threads, task execution lifecycle,
 * retries, timeout enforcement, and recurring rescheduling.
 */
public class WorkerPool {
    private static final Logger LOGGER = Logger.getLogger(WorkerPool.class.getName());

    private final int workerCount;
    private final TaskDispatchQueue dispatchQueue;
    private final Map<JobId, TaskRuntimeContext> contexts;
    private final DependencyGraph dependencyGraph;
    private final EventDispatcher eventDispatcher;
    private final SchedulerMetrics metrics;
    private final Consumer<TaskDefinition> reQueueCallback;

    private final ExecutorService workerExecutor;
    private final ScheduledExecutorService timerExecutor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public WorkerPool(int workerCount,
                      TaskDispatchQueue dispatchQueue,
                      Map<JobId, TaskRuntimeContext> contexts,
                      DependencyGraph dependencyGraph,
                      EventDispatcher eventDispatcher,
                      SchedulerMetrics metrics,
                      Consumer<TaskDefinition> reQueueCallback) {
        this.workerCount = workerCount;
        this.dispatchQueue = Objects.requireNonNull(dispatchQueue, "DispatchQueue cannot be null");
        this.contexts = Objects.requireNonNull(contexts, "Contexts cannot be null");
        this.dependencyGraph = Objects.requireNonNull(dependencyGraph, "DependencyGraph cannot be null");
        this.eventDispatcher = Objects.requireNonNull(eventDispatcher, "EventDispatcher cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.reQueueCallback = Objects.requireNonNull(reQueueCallback, "ReQueue callback cannot be null");

        AtomicInteger workerIndex = new AtomicInteger(1);
        this.workerExecutor = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "scheduler-worker-" + workerIndex.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        this.timerExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "scheduler-timer");
            t.setDaemon(true);
            return t;
        });

        // Start worker loop threads
        for (int i = 0; i < workerCount; i++) {
            this.workerExecutor.submit(this::workerLoop);
        }
    }

    private void workerLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                PrioritizedTaskItem item = dispatchQueue.poll(200, TimeUnit.MILLISECONDS);
                if (item != null) {
                    processTask(item.getTaskDefinition());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "Unexpected error in worker loop", t);
            }
        }
    }

    private void processTask(TaskDefinition task) {
        JobId jobId = task.getJobId();
        TaskRuntimeContext context = contexts.get(jobId);
        if (context == null) {
            return;
        }

        // Check if task was cancelled while in queue
        if (context.getStatus() == JobStatus.CANCELLED) {
            return;
        }

        // Check if any prerequisite dependencies failed or are incomplete
        Set<JobId> prereqs = dependencyGraph.getPrerequisites(jobId);
        if (!prereqs.isEmpty()) {
            boolean anyFailed = false;
            for (JobId prereqId : prereqs) {
                TaskRuntimeContext prereqContext = contexts.get(prereqId);
                if (prereqContext != null && (prereqContext.getStatus() == JobStatus.FAILED || prereqContext.getStatus() == JobStatus.CANCELLED)) {
                    anyFailed = true;
                    break;
                }
            }
            if (anyFailed) {
                LOGGER.warning(() -> String.format("Task %s failed because prerequisite dependency failed", jobId.value()));
                context.tryTransitionTo(JobStatus.FAILED);
                context.setLastFailure(new IllegalStateException("Prerequisite task failed"));
                metrics.incrementFailed();
                eventDispatcher.dispatch(new TaskSchedulerEvent.TaskFailed(jobId, context.getAttemptCount(), context.getLastFailure(), Instant.now()));
                return;
            }

            // Prereqs not finished yet, re-enqueue
            dispatchQueue.offer(task);
            return;
        }

        // Attempt transition to RUNNING (prevents concurrent duplicate executions)
        if (!context.tryTransitionTo(JobStatus.RUNNING)) {
            return;
        }

        int currentAttempt = context.incrementAndGetAttempts();
        context.setLastExecutionStartTime(Instant.now());
        context.setExecutingThread(Thread.currentThread());

        eventDispatcher.dispatch(new TaskSchedulerEvent.TaskStarted(jobId, Thread.currentThread().getName(), currentAttempt, Instant.now()));

        Instant startInstant = Instant.now();
        try {
            Object result;
            if (task.getExecutionTimeout() != null && !task.getExecutionTimeout().isZero()) {
                Future<Object> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return (Object) task.getExecutable().call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                result = future.get(task.getExecutionTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } else {
                result = task.getExecutable().call();
            }

            Duration duration = Duration.between(startInstant, Instant.now());
            context.setLastResult(result);
            if (!task.isRecurring()) {
                context.tryTransitionTo(JobStatus.COMPLETED);
            }
            metrics.incrementCompleted();
            eventDispatcher.dispatch(new TaskSchedulerEvent.TaskCompleted(jobId, duration, result, Instant.now()));

            // Notify dependent tasks
            onTaskSuccess(jobId);

            // Handle recurring task rescheduling
            if (task.isRecurring() && running.get()) {
                rescheduleRecurring(task);
            }

        } catch (Throwable t) {
            Throwable cause = (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
            if (cause instanceof RuntimeException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            context.setLastFailure(cause);
            Duration duration = Duration.between(startInstant, Instant.now());

            RetryPolicy retryPolicy = task.getRetryPolicy();
            if (running.get() && retryPolicy.shouldRetry(currentAttempt, cause)) {
                Duration backoff = retryPolicy.getBackoffDuration(currentAttempt);
                context.tryTransitionTo(JobStatus.RETRY_WAITING);
                metrics.incrementRetry();
                eventDispatcher.dispatch(new TaskSchedulerEvent.RetryScheduled(jobId, currentAttempt + 1, backoff, Instant.now()));

                timerExecutor.schedule(() -> {
                    if (context.getStatus() == JobStatus.RETRY_WAITING && context.tryTransitionTo(JobStatus.QUEUED)) {
                        reQueueCallback.accept(task);
                    }
                }, backoff.toMillis(), TimeUnit.MILLISECONDS);

            } else {
                context.tryTransitionTo(JobStatus.FAILED);
                metrics.incrementFailed();
                eventDispatcher.dispatch(new TaskSchedulerEvent.TaskFailed(jobId, currentAttempt, cause, Instant.now()));
            }
        } finally {
            context.setExecutingThread(null);
        }
    }

    private void onTaskSuccess(JobId completedJobId) {
        Set<JobId> dependentIds = dependencyGraph.getDependents(completedJobId);
        for (JobId depId : dependentIds) {
            dependencyGraph.removeCompletedDependency(completedJobId, depId);
        }
    }

    private void rescheduleRecurring(TaskDefinition task) {
        TaskRuntimeContext context = contexts.get(task.getJobId());
        if (context == null || context.getStatus() == JobStatus.CANCELLED) {
            return;
        }

        context.tryTransitionTo(JobStatus.QUEUED);
        long delayMillis = task.getRecurringInterval().toMillis();
        timerExecutor.schedule(() -> {
            if (context.getStatus() == JobStatus.QUEUED) {
                reQueueCallback.accept(task);
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    public void scheduleDelayed(TaskDefinition task, Duration delay) {
        timerExecutor.schedule(() -> {
            TaskRuntimeContext context = contexts.get(task.getJobId());
            if (context != null && context.getStatus() == JobStatus.QUEUED) {
                reQueueCallback.accept(task);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown(boolean immediate) {
        running.set(false);
        if (immediate) {
            workerExecutor.shutdownNow();
            timerExecutor.shutdownNow();
        } else {
            workerExecutor.shutdown();
            timerExecutor.shutdown();
            // Drain worker loops by interrupting if idle in poll
            workerExecutor.shutdownNow();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean workersDone = workerExecutor.awaitTermination(timeout, unit);
        boolean timersDone = timerExecutor.awaitTermination(timeout, unit);
        return workersDone && timersDone;
    }
}
