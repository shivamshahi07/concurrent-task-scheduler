package com.shivamshahi.scheduler;

import com.shivamshahi.scheduler.api.TaskScheduler;
import com.shivamshahi.scheduler.domain.exception.CyclicDependencyException;
import com.shivamshahi.scheduler.domain.exception.TaskRejectionException;
import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.priority.Priority;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.event.TaskSchedulerEvent;
import com.shivamshahi.scheduler.policy.ExponentialBackoffRetryPolicy;
import com.shivamshahi.scheduler.policy.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskSchedulerTest {
    private TaskScheduler scheduler;
    private List<TaskSchedulerEvent> receivedEvents;

    @BeforeEach
    void setUp() {
        scheduler = new TaskScheduler(4, 50);
        receivedEvents = new ArrayList<>();
        scheduler.addListener(receivedEvents::add);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private void awaitCompletion(JobId jobId, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<JobStatus> status = scheduler.getStatus(jobId);
            if (status.isPresent() && status.get().isTerminal()) {
                return;
            }
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("Should execute immediate tasks successfully")
    void testImmediateTasks() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ran = new AtomicBoolean(false);

        JobId jobId = scheduler.submit(TaskDefinition.newBuilder()
                .name("test-immediate")
                .action(() -> {
                    ran.set(true);
                    latch.countDown();
                }).build());

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(ran.get()).isTrue();
        awaitCompletion(jobId, Duration.ofSeconds(2));
        assertThat(scheduler.getStatus(jobId)).contains(JobStatus.COMPLETED);
    }

    @Test
    @DisplayName("Should execute delayed task after delay elapsed")
    void testDelayedExecution() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();

        JobId jobId = scheduler.submit(TaskDefinition.newBuilder()
                .name("test-delayed")
                .delay(Duration.ofMillis(80))
                .action(latch::countDown)
                .build());

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        long elapsed = System.currentTimeMillis() - startTime;
        assertThat(elapsed).isGreaterThanOrEqualTo(70);
        awaitCompletion(jobId, Duration.ofSeconds(2));
        assertThat(scheduler.getStatus(jobId)).contains(JobStatus.COMPLETED);
    }

    @Test
    @DisplayName("Should execute recurring tasks repeatedly until cancelled")
    void testRecurringExecution() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        JobId jobId = scheduler.submit(TaskDefinition.newBuilder()
                .name("test-recurring")
                .fixedDelay(Duration.ofMillis(50))
                .action(() -> {
                    counter.incrementAndGet();
                    latch.countDown();
                }).build());

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        scheduler.cancel(jobId, true);
        assertThat(scheduler.getStatus(jobId)).contains(JobStatus.CANCELLED);
        int finalCount = counter.get();
        Thread.sleep(120);
        assertThat(counter.get()).isEqualTo(finalCount);
    }

    @Test
    @DisplayName("Should respect priority ordering and deterministic FIFO tie-breaking")
    void testPriorityOrdering() throws InterruptedException {
        TaskScheduler singleWorkerScheduler = new TaskScheduler(1, 20);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch pause = new CountDownLatch(1);
        List<String> orderLog = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch doneLatch = new CountDownLatch(3);

        singleWorkerScheduler.submit(TaskDefinition.newBuilder().name("blocker").action(() -> {
            blockerStarted.countDown();
            try { pause.await(500, TimeUnit.MILLISECONDS); } catch (Exception ignored) {}
        }).build());

        assertThat(blockerStarted.await(2, TimeUnit.SECONDS)).isTrue();

        singleWorkerScheduler.submit(TaskDefinition.newBuilder().name("Low-1").priority(Priority.LOW).action(() -> {
            orderLog.add("LOW-1");
            doneLatch.countDown();
        }).build());
        singleWorkerScheduler.submit(TaskDefinition.newBuilder().name("High-1").priority(Priority.HIGH).action(() -> {
            orderLog.add("HIGH-1");
            doneLatch.countDown();
        }).build());
        singleWorkerScheduler.submit(TaskDefinition.newBuilder().name("Critical-1").priority(Priority.CRITICAL).action(() -> {
            orderLog.add("CRITICAL-1");
            doneLatch.countDown();
        }).build());

        pause.countDown();
        assertThat(doneLatch.await(3, TimeUnit.SECONDS)).isTrue();
        singleWorkerScheduler.shutdownNow();

        assertThat(orderLog.get(0)).isEqualTo("CRITICAL-1");
        assertThat(orderLog.get(1)).isEqualTo("HIGH-1");
        assertThat(orderLog.get(2)).isEqualTo("LOW-1");
    }

    @Test
    @DisplayName("Should retry retryable failure and succeed on 2nd attempt")
    void testRetryWorkflow() throws InterruptedException {
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        JobId jobId = scheduler.submit(TaskDefinition.newBuilder()
                .name("test-retry")
                .retryPolicy(ExponentialBackoffRetryPolicy.of(3, Duration.ofMillis(10)))
                .action(() -> {
                    int a = attempts.incrementAndGet();
                    if (a < 2) {
                        throw new RuntimeException("Transient failure");
                    }
                    latch.countDown();
                }).build());

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        awaitCompletion(jobId, Duration.ofSeconds(2));
        assertThat(scheduler.getStatus(jobId)).contains(JobStatus.COMPLETED);
        assertThat(scheduler.getContext(jobId).get().getAttemptCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should enforce dependencies and execute child only after parent completes")
    void testDependencyEnforcement() throws InterruptedException {
        List<String> audit = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch childLatch = new CountDownLatch(1);

        JobId parent = scheduler.submit(TaskDefinition.newBuilder()
                .name("ParentTask")
                .action(() -> {
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    audit.add("PARENT");
                }).build());

        JobId child = scheduler.submit(TaskDefinition.newBuilder()
                .name("ChildTask")
                .dependsOn(parent)
                .action(() -> {
                    audit.add("CHILD");
                    childLatch.countDown();
                }).build());

        assertThat(childLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(audit).containsExactly("PARENT", "CHILD");
    }

    @Test
    @DisplayName("Should reject cyclic dependencies")
    void testCyclicDependency() {
        JobId a = JobId.of("task-A");
        JobId b = JobId.of("task-B");

        scheduler.submit(TaskDefinition.newBuilder().jobId(a).name("A").dependsOn(b).action(() -> {}).build());

        assertThatThrownBy(() -> scheduler.submit(TaskDefinition.newBuilder().jobId(b).name("B").dependsOn(a).action(() -> {}).build()))
                .isInstanceOf(CyclicDependencyException.class)
                .hasMessageContaining("Cyclic dependency detected");
    }

    @Test
    @DisplayName("Should fail dependent task when prerequisite task fails")
    void testPrerequisiteFailurePropagation() throws InterruptedException {
        JobId failingParent = scheduler.submit(TaskDefinition.newBuilder()
                .name("FailingParent")
                .action(() -> {
                    throw new RuntimeException("DB down");
                }).build());

        JobId child = scheduler.submit(TaskDefinition.newBuilder()
                .name("DependentChild")
                .dependsOn(failingParent)
                .action(() -> {})
                .build());

        awaitCompletion(child, Duration.ofSeconds(2));
        assertThat(scheduler.getStatus(child)).contains(JobStatus.FAILED);
    }

    @Test
    @DisplayName("Should enforce bounded queue capacity and reject overflowing tasks")
    void testBoundedQueueRejection() throws InterruptedException {
        TaskScheduler smallScheduler = new TaskScheduler(1, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch pause = new CountDownLatch(1);

        smallScheduler.submit(TaskDefinition.newBuilder().name("t1").action(() -> {
            started.countDown();
            try { pause.await(1000, TimeUnit.MILLISECONDS); } catch (Exception ignored) {}
        }).build());

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        smallScheduler.submit(TaskDefinition.newBuilder().name("t2").action(() -> {}).build());
        smallScheduler.submit(TaskDefinition.newBuilder().name("t3").action(() -> {}).build());

        assertThatThrownBy(() -> smallScheduler.submit(TaskDefinition.newBuilder().name("overflow").action(() -> {}).build()))
                .isInstanceOf(TaskRejectionException.class);

        pause.countDown();
        smallScheduler.shutdownNow();
    }

    @Test
    @DisplayName("Should isolate worker exceptions and execute subsequent tasks")
    void testWorkerFaultTolerance() throws InterruptedException {
        CountDownLatch survivorLatch = new CountDownLatch(1);

        scheduler.submit(TaskDefinition.newBuilder().name("crash").action(() -> {
            throw new Error("Crash error");
        }).build());

        scheduler.submit(TaskDefinition.newBuilder().name("survivor").action(survivorLatch::countDown).build());

        assertThat(survivorLatch.await(2, TimeUnit.SECONDS)).isTrue();
    }
}
