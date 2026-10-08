package com.shivamshahi.scheduler.domain;

import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.priority.Priority;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.policy.ExponentialBackoffRetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DomainAndPolicyTest {

    @Test
    @DisplayName("Test JobId value object validation")
    void testJobId() {
        JobId id = JobId.of("custom_1");
        assertThat(id.value()).isEqualTo("custom_1");

        assertThatThrownBy(() -> JobId.of(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Test JobStatus state transitions")
    void testJobStatusTransitions() {
        assertThat(JobStatus.QUEUED.canTransitionTo(JobStatus.RUNNING)).isTrue();
        assertThat(JobStatus.QUEUED.canTransitionTo(JobStatus.CANCELLED)).isTrue();
        assertThat(JobStatus.QUEUED.canTransitionTo(JobStatus.FAILED)).isTrue();

        assertThat(JobStatus.RUNNING.canTransitionTo(JobStatus.COMPLETED)).isTrue();
        assertThat(JobStatus.RUNNING.canTransitionTo(JobStatus.FAILED)).isTrue();
        assertThat(JobStatus.RUNNING.canTransitionTo(JobStatus.RETRY_WAITING)).isTrue();

        assertThat(JobStatus.RETRY_WAITING.canTransitionTo(JobStatus.QUEUED)).isTrue();
        assertThat(JobStatus.RETRY_WAITING.canTransitionTo(JobStatus.CANCELLED)).isTrue();

        assertThat(JobStatus.COMPLETED.isTerminal()).isTrue();
        assertThat(JobStatus.FAILED.isTerminal()).isTrue();
        assertThat(JobStatus.CANCELLED.isTerminal()).isTrue();
        assertThat(JobStatus.COMPLETED.canTransitionTo(JobStatus.RUNNING)).isFalse();
    }

    @Test
    @DisplayName("Test Priority enum levels")
    void testPriorityLevels() {
        assertThat(Priority.CRITICAL.getLevel()).isGreaterThan(Priority.HIGH.getLevel());
        assertThat(Priority.HIGH.getLevel()).isGreaterThan(Priority.NORMAL.getLevel());
        assertThat(Priority.NORMAL.getLevel()).isGreaterThan(Priority.LOW.getLevel());
    }

    @Test
    @DisplayName("Test ExponentialBackoffRetryPolicy with fatal exceptions")
    void testRetryPolicy() {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(
                3, Duration.ofMillis(10), 2.0, Duration.ofMillis(100), Set.of(IllegalArgumentException.class)
        );

        assertThat(policy.getMaxAttempts()).isEqualTo(3);
        assertThat(policy.shouldRetry(1, new IOException("network down"))).isTrue();
        assertThat(policy.shouldRetry(1, new IllegalArgumentException("bad argument"))).isFalse();
        assertThat(policy.shouldRetry(3, new IOException("network down"))).isFalse();

        assertThat(policy.getBackoffDuration(1)).isEqualTo(Duration.ofMillis(10));
        assertThat(policy.getBackoffDuration(2)).isEqualTo(Duration.ofMillis(20));
        assertThat(policy.getBackoffDuration(3)).isEqualTo(Duration.ofMillis(40));
    }

    @Test
    @DisplayName("Test TaskDefinition builder options")
    void testTaskDefinitionBuilder() {
        TaskDefinition def = TaskDefinition.newBuilder()
                .name("custom-task")
                .priority(Priority.HIGH)
                .fixedRate(Duration.ofSeconds(5))
                .executionTimeout(Duration.ofSeconds(10))
                .scheduledAt(Instant.now())
                .action(() -> {})
                .build();

        assertThat(def.getName()).isEqualTo("custom-task");
        assertThat(def.getPriority()).isEqualTo(Priority.HIGH);
        assertThat(def.isRecurring()).isTrue();
        assertThat(def.isFixedRate()).isTrue();
        assertThat(def.getExecutionTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(def.getJobId()).isNotNull();
    }
}
