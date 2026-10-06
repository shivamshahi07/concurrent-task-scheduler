package com.shivamshahi.scheduler.domain.model;

import com.shivamshahi.scheduler.domain.priority.Priority;
import com.shivamshahi.scheduler.policy.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Task specification representing the immutable configuration of a task.
 */
public final class TaskDefinition {
    private final JobId jobId;
    private final String name;
    private final Callable<?> executable;
    private final Priority priority;
    private final Instant scheduledExecutionTime;
    private final Duration recurringInterval;
    private final boolean fixedRate;
    private final RetryPolicy retryPolicy;
    private final Duration executionTimeout;
    private final Set<JobId> dependencies;

    private TaskDefinition(Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId, "JobId cannot be null");
        this.name = Objects.requireNonNull(builder.name, "Task name cannot be null");
        this.executable = Objects.requireNonNull(builder.executable, "Task executable cannot be null");
        this.priority = Objects.requireNonNull(builder.priority, "Task priority cannot be null");
        this.scheduledExecutionTime = builder.scheduledExecutionTime != null ? builder.scheduledExecutionTime : Instant.now();
        this.recurringInterval = builder.recurringInterval;
        this.fixedRate = builder.fixedRate;
        this.retryPolicy = builder.retryPolicy != null ? builder.retryPolicy : RetryPolicy.noRetries();
        this.executionTimeout = builder.executionTimeout;
        this.dependencies = Collections.unmodifiableSet(new HashSet<>(builder.dependencies));
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public JobId getJobId() {
        return jobId;
    }

    public String getName() {
        return name;
    }

    public Callable<?> getExecutable() {
        return executable;
    }

    public Priority getPriority() {
        return priority;
    }

    public Instant getScheduledExecutionTime() {
        return scheduledExecutionTime;
    }

    public Duration getRecurringInterval() {
        return recurringInterval;
    }

    public boolean isRecurring() {
        return recurringInterval != null && !recurringInterval.isZero() && !recurringInterval.isNegative();
    }

    public boolean isFixedRate() {
        return fixedRate;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public Duration getExecutionTimeout() {
        return executionTimeout;
    }

    public Set<JobId> getDependencies() {
        return dependencies;
    }

    public static class Builder {
        private JobId jobId;
        private String name;
        private Callable<?> executable;
        private Priority priority = Priority.NORMAL;
        private Instant scheduledExecutionTime;
        private Duration recurringInterval;
        private boolean fixedRate = false;
        private RetryPolicy retryPolicy;
        private Duration executionTimeout;
        private final Set<JobId> dependencies = new HashSet<>();

        public Builder jobId(JobId jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder action(Runnable runnable) {
            Objects.requireNonNull(runnable, "Runnable cannot be null");
            this.executable = () -> {
                runnable.run();
                return null;
            };
            return this;
        }

        public Builder action(Callable<?> callable) {
            this.executable = callable;
            return this;
        }

        public Builder priority(Priority priority) {
            this.priority = priority;
            return this;
        }

        public Builder scheduledAt(Instant instant) {
            this.scheduledExecutionTime = instant;
            return this;
        }

        public Builder delay(Duration delay) {
            this.scheduledExecutionTime = Instant.now().plus(delay);
            return this;
        }

        public Builder fixedDelay(Duration interval) {
            this.recurringInterval = interval;
            this.fixedRate = false;
            return this;
        }

        public Builder fixedRate(Duration interval) {
            this.recurringInterval = interval;
            this.fixedRate = true;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder executionTimeout(Duration timeout) {
            this.executionTimeout = timeout;
            return this;
        }

        public Builder dependsOn(JobId dependencyId) {
            this.dependencies.add(Objects.requireNonNull(dependencyId, "Dependency cannot be null"));
            return this;
        }

        public TaskDefinition build() {
            if (jobId == null) {
                jobId = JobId.generate();
            }
            if (name == null || name.isBlank()) {
                name = "task-" + jobId.value();
            }
            return new TaskDefinition(this);
        }
    }
}
