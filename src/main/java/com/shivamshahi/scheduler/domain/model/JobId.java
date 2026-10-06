package com.shivamshahi.scheduler.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Strongly typed unique identifier for a scheduled task/job.
 */
public record JobId(String value) {
    public JobId {
        Objects.requireNonNull(value, "JobId value cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("JobId cannot be blank");
        }
    }

    public static JobId generate() {
        return new JobId("job_" + UUID.randomUUID().toString().replace("-", ""));
    }

    public static JobId of(String value) {
        return new JobId(value.trim());
    }
}
