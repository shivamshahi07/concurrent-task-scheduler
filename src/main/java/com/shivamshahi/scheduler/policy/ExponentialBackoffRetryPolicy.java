package com.shivamshahi.scheduler.policy;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * Exponential backoff retry policy distinguishing retryable from fatal exceptions.
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    private final int maxAttempts;
    private final Duration initialDelay;
    private final double backoffMultiplier;
    private final Duration maxDelay;
    private final Set<Class<? extends Throwable>> fatalExceptions;

    public ExponentialBackoffRetryPolicy(int maxAttempts,
                                         Duration initialDelay,
                                         double backoffMultiplier,
                                         Duration maxDelay,
                                         Set<Class<? extends Throwable>> fatalExceptions) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("Max attempts must be at least 1");
        }
        this.maxAttempts = maxAttempts;
        this.initialDelay = Objects.requireNonNull(initialDelay, "Initial delay cannot be null");
        this.backoffMultiplier = backoffMultiplier;
        this.maxDelay = Objects.requireNonNull(maxDelay, "Max delay cannot be null");
        this.fatalExceptions = fatalExceptions != null ? fatalExceptions : Set.of();
    }

    public static ExponentialBackoffRetryPolicy of(int maxAttempts, Duration initialDelay) {
        return new ExponentialBackoffRetryPolicy(maxAttempts, initialDelay, 2.0, Duration.ofMinutes(1), Set.of());
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public boolean shouldRetry(int currentAttempt, Throwable throwable) {
        if (currentAttempt >= maxAttempts) {
            return false;
        }
        if (throwable != null) {
            for (Class<? extends Throwable> fatal : fatalExceptions) {
                if (fatal.isAssignableFrom(throwable.getClass())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Duration getBackoffDuration(int currentAttempt) {
        if (currentAttempt <= 1 || initialDelay.isZero()) {
            return initialDelay;
        }
        long millis = (long) (initialDelay.toMillis() * Math.pow(backoffMultiplier, currentAttempt - 1));
        if (millis > maxDelay.toMillis()) {
            return maxDelay;
        }
        return Duration.ofMillis(millis);
    }
}
