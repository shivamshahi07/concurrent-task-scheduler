package com.shivamshahi.scheduler.core.queue;

import com.shivamshahi.scheduler.domain.model.TaskDefinition;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Task item holding a sequence number to guarantee deterministic FIFO tie-breaking for equal priorities.
 */
public final class PrioritizedTaskItem implements Comparable<PrioritizedTaskItem> {
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private final TaskDefinition taskDefinition;
    private final long sequenceNumber;

    public PrioritizedTaskItem(TaskDefinition taskDefinition) {
        this.taskDefinition = Objects.requireNonNull(taskDefinition, "TaskDefinition cannot be null");
        this.sequenceNumber = SEQUENCE_GENERATOR.getAndIncrement();
    }

    public TaskDefinition getTaskDefinition() {
        return taskDefinition;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public int compareTo(PrioritizedTaskItem o) {
        // Higher priority first
        int cmp = Integer.compare(o.taskDefinition.getPriority().getLevel(), this.taskDefinition.getPriority().getLevel());
        if (cmp != 0) {
            return cmp;
        }
        // Deterministic FIFO tie-breaking: lower sequence number was submitted earlier
        return Long.compare(this.sequenceNumber, o.sequenceNumber);
    }
}
