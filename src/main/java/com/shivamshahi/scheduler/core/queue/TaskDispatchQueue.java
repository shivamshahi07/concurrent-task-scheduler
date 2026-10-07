package com.shivamshahi.scheduler.core.queue;

import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe Priority-based task dispatch queue with bounded admission control.
 */
public class TaskDispatchQueue {
    private final int capacity;
    private final PriorityBlockingQueue<PrioritizedTaskItem> queue;

    public TaskDispatchQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Queue capacity must be at least 1");
        }
        this.capacity = capacity;
        this.queue = new PriorityBlockingQueue<>(capacity);
    }

    public synchronized boolean offer(TaskDefinition taskDefinition) {
        Objects.requireNonNull(taskDefinition, "TaskDefinition cannot be null");
        if (queue.size() >= capacity) {
            return false;
        }
        return queue.offer(new PrioritizedTaskItem(taskDefinition));
    }

    public PrioritizedTaskItem poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public PrioritizedTaskItem poll() {
        return queue.poll();
    }

    public synchronized boolean remove(JobId jobId) {
        return queue.removeIf(item -> item.getTaskDefinition().getJobId().equals(jobId));
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
