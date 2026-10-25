package com.shivamshahi.scheduler.api;

import com.shivamshahi.scheduler.core.dependency.DependencyGraph;
import com.shivamshahi.scheduler.core.execution.WorkerPool;
import com.shivamshahi.scheduler.core.lifecycle.TaskRuntimeContext;
import com.shivamshahi.scheduler.core.queue.TaskDispatchQueue;
import com.shivamshahi.scheduler.domain.exception.TaskRejectionException;
import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.event.EventDispatcher;
import com.shivamshahi.scheduler.event.TaskLifecycleListener;
import com.shivamshahi.scheduler.event.TaskSchedulerEvent;
import com.shivamshahi.scheduler.metrics.SchedulerMetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Public Thread-Safe Scheduler Engine interface implementation.
 */
public class TaskScheduler {
    private static final Logger LOGGER = Logger.getLogger(TaskScheduler.class.getName());

    private final TaskDispatchQueue dispatchQueue;
    private final Map<JobId, TaskRuntimeContext> contexts = new ConcurrentHashMap<>();
    private final Map<JobId, TaskDefinition> taskDefinitions = new ConcurrentHashMap<>();
    private final DependencyGraph dependencyGraph = new DependencyGraph();
    private final EventDispatcher eventDispatcher = new EventDispatcher();
    private final SchedulerMetrics metrics = new SchedulerMetrics();
    private final WorkerPool workerPool;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public TaskScheduler(int workerCount, int queueCapacity) {
        this.dispatchQueue = new TaskDispatchQueue(queueCapacity);
        this.workerPool = new WorkerPool(
                workerCount,
                this.dispatchQueue,
                this.contexts,
                this.dependencyGraph,
                this.eventDispatcher,
                this.metrics,
                this::dispatchTask
        );
    }

    public static TaskScheduler createDefault() {
        return new TaskScheduler(4, 100);
    }

    public void addListener(TaskLifecycleListener listener) {
        eventDispatcher.addListener(listener);
    }

    public JobId submit(TaskDefinition task) {
        Objects.requireNonNull(task, "TaskDefinition cannot be null");
        if (isShutdown.get()) {
            throw new TaskRejectionException("Scheduler has been shut down; new tasks cannot be accepted");
        }

        JobId jobId = task.getJobId();
        taskDefinitions.put(jobId, task);

        TaskRuntimeContext context = new TaskRuntimeContext(jobId, task.getScheduledExecutionTime());
        contexts.put(jobId, context);
        metrics.incrementSubmitted();

        // Register dependencies (detects cycles before queuing)
        if (!task.getDependencies().isEmpty()) {
            dependencyGraph.registerDependencies(jobId, task.getDependencies());
        }

        eventDispatcher.dispatch(new TaskSchedulerEvent.TaskSubmitted(jobId, task.getName(), Instant.now()));

        // Check if execution is delayed
        Duration delay = Duration.between(Instant.now(), task.getScheduledExecutionTime());
        if (!delay.isNegative() && !delay.isZero()) {
            workerPool.scheduleDelayed(task, delay);
        } else {
            dispatchTask(task);
        }

        return jobId;
    }

    private void dispatchTask(TaskDefinition task) {
        if (isShutdown.get()) {
            return;
        }
        boolean accepted = dispatchQueue.offer(task);
        if (!accepted) {
            TaskRuntimeContext ctx = contexts.get(task.getJobId());
            if (ctx != null && ctx.tryTransitionTo(JobStatus.FAILED)) {
                ctx.setLastFailure(new TaskRejectionException("Scheduler queue capacity exceeded"));
                metrics.incrementFailed();
                eventDispatcher.dispatch(new TaskSchedulerEvent.TaskFailed(task.getJobId(), 0, ctx.getLastFailure(), Instant.now()));
            }
            throw new TaskRejectionException("Task queue is full; cannot accept task " + task.getJobId().value());
        }
    }

    public boolean cancel(JobId jobId, boolean mayInterruptIfRunning) {
        Objects.requireNonNull(jobId, "JobId cannot be null");
        TaskRuntimeContext context = contexts.get(jobId);
        if (context == null) {
            return false;
        }

        JobStatus currentStatus = context.getStatus();
        if (currentStatus.isTerminal()) {
            return false;
        }

        // Try transitioning to CANCELLED
        if (currentStatus == JobStatus.QUEUED || currentStatus == JobStatus.RETRY_WAITING) {
            if (context.tryTransitionTo(JobStatus.CANCELLED)) {
                dispatchQueue.remove(jobId);
                metrics.incrementCancelled();
                eventDispatcher.dispatch(new TaskSchedulerEvent.TaskCancelled(jobId, currentStatus, Instant.now()));
                return true;
            }
        } else if (currentStatus == JobStatus.RUNNING) {
            if (mayInterruptIfRunning) {
                Thread workerThread = context.getExecutingThread();
                if (workerThread != null) {
                    workerThread.interrupt();
                }
            }
            if (context.tryTransitionTo(JobStatus.CANCELLED)) {
                metrics.incrementCancelled();
                eventDispatcher.dispatch(new TaskSchedulerEvent.TaskCancelled(jobId, currentStatus, Instant.now()));
                return true;
            }
        }
        return false;
    }

    public Optional<JobStatus> getStatus(JobId jobId) {
        TaskRuntimeContext ctx = contexts.get(jobId);
        return ctx == null ? Optional.empty() : Optional.of(ctx.getStatus());
    }

    public Optional<TaskRuntimeContext> getContext(JobId jobId) {
        return Optional.ofNullable(contexts.get(jobId));
    }

    public SchedulerMetrics getMetrics() {
        return metrics;
    }

    public int getQueueDepth() {
        return dispatchQueue.size();
    }

    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            workerPool.shutdown(false);
        }
    }

    public void shutdownNow() {
        if (isShutdown.compareAndSet(false, true)) {
            workerPool.shutdown(true);
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return workerPool.awaitTermination(timeout, unit);
    }
}
