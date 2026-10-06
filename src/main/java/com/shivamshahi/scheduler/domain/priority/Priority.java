package com.shivamshahi.scheduler.domain.priority;

/**
 * Task priority levels.
 * HIGHEST priority tasks are dispatched before LOWER priority tasks when eligible.
 */
public enum Priority {
    CRITICAL(4),
    HIGH(3),
    NORMAL(2),
    LOW(1);

    private final int level;

    Priority(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
