package com.shivamshahi.scheduler.policy;

import java.time.Duration;

public interface RetryPolicy {
    int getMaxAttempts();
    boolean shouldRetry(int currentAttempt, Throwable throwable);
    Duration getBackoffDuration(int currentAttempt);

    static RetryPolicy noRetries() {
        return new RetryPolicy() {
            @Override
            public int getMaxAttempts() {
                return 1;
            }

            @Override
            public boolean shouldRetry(int currentAttempt, Throwable throwable) {
                return false;
            }

            @Override
            public Duration getBackoffDuration(int currentAttempt) {
                return Duration.ZERO;
            }
        };
    }
}
