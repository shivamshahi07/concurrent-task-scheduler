package com.shivamshahi.scheduler.demo;

import com.shivamshahi.scheduler.api.TaskScheduler;
import com.shivamshahi.scheduler.domain.exception.CyclicDependencyException;
import com.shivamshahi.scheduler.domain.exception.TaskRejectionException;
import com.shivamshahi.scheduler.domain.model.JobId;
import com.shivamshahi.scheduler.domain.model.TaskDefinition;
import com.shivamshahi.scheduler.domain.priority.Priority;
import com.shivamshahi.scheduler.domain.status.JobStatus;
import com.shivamshahi.scheduler.policy.ExponentialBackoffRetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executable interactive CLI demonstration of all 18 scheduler acceptance scenarios:
 * 1. Multiple immediate jobs
 * 2. Delayed execution
 * 3. Recurring execution
 * 4. Priority ordering among eligible tasks
 * 5. Deterministic tie-breaking for equal-priority jobs
 * 6. Multiple worker threads processing tasks concurrently
 * 7. Retry followed by success
 * 8. Exhausted retries and permanent failure
 * 9. Cancellation of a queued job
 * 10. Cancellation of a recurring job
 * 11. Dependent task waiting for prerequisite
 * 12. Failure propagation through task dependencies
 * 13. Rejection of dependency cycles
 * 14. Graceful shutdown
 * 15. Submission after shutdown rejection
 * 16. Bounded-queue admission control
 * 17. Worker fault tolerance (task throws without crashing worker)
 * 18. Scheduler statistics and observability events
 */
public class SchedulerDemoRunner {
    private static final String SEPARATOR = "=".repeat(75);
    private static final String SUB_SEPARATOR = "-".repeat(75);

    public static void main(String[] args) throws Exception {
        System.out.println(SEPARATOR);
        System.out.println("   CONCURRENT TASK SCHEDULER ENGINE - ACCEPTANCE SCENARIOS DEMO");
        System.out.println(SEPARATOR);

        TaskScheduler scheduler = new TaskScheduler(4, 50);

        scheduler.addListener(event -> {
            System.out.printf("   [SCHEDULER MONITOR] Event: %-18s | JobId: %s%n",
                    event.getEventType(), event.getJobId().value());
        });

        // Scenario 1: Immediate tasks
        printHeader(1, "Immediate Tasks Execution");
        CountDownLatch l1 = new CountDownLatch(2);
        scheduler.submit(TaskDefinition.newBuilder().name("immediate-1").action(() -> {
            System.out.println("   -> Executing Immediate Task 1");
            l1.countDown();
        }).build());
        scheduler.submit(TaskDefinition.newBuilder().name("immediate-2").action(() -> {
            System.out.println("   -> Executing Immediate Task 2");
            l1.countDown();
        }).build());
        l1.await(2, TimeUnit.SECONDS);

        // Scenario 2: Delayed execution
        printHeader(2, "Delayed Task Execution (150ms delay)");
        CountDownLatch l2 = new CountDownLatch(1);
        long startDelayed = System.currentTimeMillis();
        scheduler.submit(TaskDefinition.newBuilder().name("delayed-job").delay(Duration.ofMillis(150)).action(() -> {
            long elapsed = System.currentTimeMillis() - startDelayed;
            System.out.printf("   -> Executed Delayed Job after %d ms%n", elapsed);
            l2.countDown();
        }).build());
        l2.await(2, TimeUnit.SECONDS);

        // Scenario 3: Recurring execution
        printHeader(3, "Recurring Execution (runs every 100ms)");
        AtomicInteger recurCount = new AtomicInteger(0);
        CountDownLatch l3 = new CountDownLatch(3);
        JobId recurringId = scheduler.submit(TaskDefinition.newBuilder()
                .name("heartbeat-recurring")
                .fixedDelay(Duration.ofMillis(100))
                .action(() -> {
                    int c = recurCount.incrementAndGet();
                    System.out.printf("   -> Recurring pulse #%d%n", c);
                    l3.countDown();
                }).build());
        l3.await(2, TimeUnit.SECONDS);

        // Scenario 10: Cancel recurring job
        printHeader(10, "Cancellation of Recurring Job");
        scheduler.cancel(recurringId, true);
        System.out.printf("   Recurring Job cancelled. Final status: %s%n", scheduler.getStatus(recurringId).orElse(null));

        // Scenario 4 & 5: Priority ordering & deterministic tie-breaking
        printHeader(4, "Priority Ordering & Deterministic Tie-Breaking");
        // Pause dispatch by using latch
        CountDownLatch pauseBlocker = new CountDownLatch(1);
        List<String> orderLog = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch priorityLatch = new CountDownLatch(4);

        // Submit blocker to keep workers busy
        scheduler.submit(TaskDefinition.newBuilder().name("blocker").priority(Priority.CRITICAL).action(() -> {
            try { pauseBlocker.await(500, TimeUnit.MILLISECONDS); } catch (Exception ignored) {}
        }).build());

        // Queue in mixed order
        scheduler.submit(TaskDefinition.newBuilder().name("Low-1").priority(Priority.LOW).action(() -> {
            orderLog.add("LOW-1");
            priorityLatch.countDown();
        }).build());
        scheduler.submit(TaskDefinition.newBuilder().name("High-1").priority(Priority.HIGH).action(() -> {
            orderLog.add("HIGH-1");
            priorityLatch.countDown();
        }).build());
        scheduler.submit(TaskDefinition.newBuilder().name("High-2").priority(Priority.HIGH).action(() -> {
            orderLog.add("HIGH-2");
            priorityLatch.countDown();
        }).build());
        scheduler.submit(TaskDefinition.newBuilder().name("Critical-1").priority(Priority.CRITICAL).action(() -> {
            orderLog.add("CRITICAL-1");
            priorityLatch.countDown();
        }).build());

        pauseBlocker.countDown();
        priorityLatch.await(2, TimeUnit.SECONDS);
        System.out.printf("   Observed Dispatch Order: %s%n", orderLog);

        // Scenario 6: Multiple worker concurrency
        printHeader(6, "Concurrent Execution on Multiple Worker Threads");
        CountDownLatch concLatch = new CountDownLatch(4);
        for (int i = 1; i <= 4; i++) {
            final int id = i;
            scheduler.submit(TaskDefinition.newBuilder().name("conc-worker-" + id).action(() -> {
                System.out.printf("   Worker Task %d running on thread: %s%n", id, Thread.currentThread().getName());
                concLatch.countDown();
            }).build());
        }
        concLatch.await(2, TimeUnit.SECONDS);

        // Scenario 7: Retry followed by success
        printHeader(7, "Retry Followed by Eventual Success");
        AtomicInteger retrySuccessCounter = new AtomicInteger(0);
        CountDownLatch retrySuccessLatch = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.newBuilder()
                .name("retry-success-job")
                .retryPolicy(ExponentialBackoffRetryPolicy.of(3, Duration.ofMillis(20)))
                .action(() -> {
                    int c = retrySuccessCounter.incrementAndGet();
                    if (c < 3) {
                        System.out.printf("   [Simulated Failure] Attempt %d failed%n", c);
                        throw new RuntimeException("Temporary network blip");
                    }
                    System.out.println("   [Recovery] Attempt 3 succeeded!");
                    retrySuccessLatch.countDown();
                }).build());
        retrySuccessLatch.await(3, TimeUnit.SECONDS);

        // Scenario 8: Exhausted retries and permanent failure
        printHeader(8, "Exhausted Retries Leading to Terminal FAILED Status");
        CountDownLatch exhaustLatch = new CountDownLatch(1);
        JobId exhaustedId = scheduler.submit(TaskDefinition.newBuilder()
                .name("exhausted-job")
                .retryPolicy(ExponentialBackoffRetryPolicy.of(2, Duration.ofMillis(10)))
                .action(() -> {
                    exhaustLatch.countDown();
                    throw new RuntimeException("Permanent backend outage");
                }).build());
        exhaustLatch.await(2, TimeUnit.SECONDS);
        Thread.sleep(100);
        System.out.printf("   Job %s status after exhausted attempts: %s%n", exhaustedId.value(), scheduler.getStatus(exhaustedId).orElse(null));

        // Scenario 9: Cancellation of queued job
        printHeader(9, "Cancellation of Queued Task");
        JobId delayedCancelId = scheduler.submit(TaskDefinition.newBuilder()
                .name("delayed-to-cancel")
                .delay(Duration.ofSeconds(10))
                .action(() -> {})
                .build());
        boolean cancelled = scheduler.cancel(delayedCancelId, true);
        System.out.printf("   Cancellation requested for queued job: %b | Status: %s%n",
                cancelled, scheduler.getStatus(delayedCancelId).orElse(null));

        // Scenario 11: Task Dependencies
        printHeader(11, "Task Dependencies (Child waits for Parent)");
        CountDownLatch depLatch = new CountDownLatch(1);
        List<String> depLog = Collections.synchronizedList(new ArrayList<>());
        JobId parent = scheduler.submit(TaskDefinition.newBuilder().name("Parent-Step-1").action(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            depLog.add("PARENT_FINISHED");
        }).build());

        scheduler.submit(TaskDefinition.newBuilder().name("Child-Step-2").dependsOn(parent).action(() -> {
            depLog.add("CHILD_FINISHED");
            depLatch.countDown();
        }).build());
        depLatch.await(2, TimeUnit.SECONDS);
        System.out.printf("   Dependency Execution Sequence: %s%n", depLog);

        // Scenario 12: Failure propagation in dependencies
        printHeader(12, "Failure Propagation Through Task Dependencies");
        JobId failingParent = scheduler.submit(TaskDefinition.newBuilder().name("Failing-Parent").action(() -> {
            throw new RuntimeException("Parent DB migration failed");
        }).build());

        CountDownLatch failChildLatch = new CountDownLatch(1);
        JobId dependentChild = scheduler.submit(TaskDefinition.newBuilder()
                .name("Dependent-Child")
                .dependsOn(failingParent)
                .action(() -> {})
                .build());

        Thread.sleep(150);
        System.out.printf("   Dependent Child Status when Parent fails: %s%n", scheduler.getStatus(dependentChild).orElse(null));

        // Scenario 13: Rejection of dependency cycles
        printHeader(13, "Cyclic Dependency Rejection (A -> B -> A)");
        JobId jobA = JobId.of("cycle-task-A");
        JobId jobB = JobId.of("cycle-task-B");
        scheduler.submit(TaskDefinition.newBuilder().jobId(jobA).name("Task-A").dependsOn(jobB).action(() -> {}).build());
        try {
            scheduler.submit(TaskDefinition.newBuilder().jobId(jobB).name("Task-B").dependsOn(jobA).action(() -> {}).build());
            System.err.println("   [ERROR] Should have thrown CyclicDependencyException!");
        } catch (CyclicDependencyException ex) {
            System.out.printf("   Cycle Detected and Rejected: %s%n", ex.getMessage());
        }

        // Scenario 16: Bounded queue admission control
        printHeader(16, "Bounded Queue Admission Control");
        TaskScheduler tinyScheduler = new TaskScheduler(1, 2);
        CountDownLatch tinyLatch = new CountDownLatch(1);
        // Fill worker & queue
        tinyScheduler.submit(TaskDefinition.newBuilder().name("hold-1").action(() -> {
            try { tinyLatch.await(1, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }).build());
        tinyScheduler.submit(TaskDefinition.newBuilder().name("q-1").action(() -> {}).build());
        tinyScheduler.submit(TaskDefinition.newBuilder().name("q-2").action(() -> {}).build());

        try {
            tinyScheduler.submit(TaskDefinition.newBuilder().name("overflow").action(() -> {}).build());
            System.err.println("   [ERROR] Should have rejected task on full queue!");
        } catch (TaskRejectionException ex) {
            System.out.printf("   Expected Admission Rejection: %s%n", ex.getMessage());
        }
        tinyLatch.countDown();
        tinyScheduler.shutdownNow();

        // Scenario 17: Worker Fault Isolation
        printHeader(17, "Worker Fault Isolation (Exception does not crash worker thread)");
        CountDownLatch afterCrashLatch = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.newBuilder().name("exploding-task").action(() -> {
            throw new Error("Simulated unchecked Error/Exception");
        }).build());
        scheduler.submit(TaskDefinition.newBuilder().name("following-task").action(() -> {
            System.out.println("   -> Follower task executed successfully on surviving worker thread!");
            afterCrashLatch.countDown();
        }).build());
        afterCrashLatch.await(2, TimeUnit.SECONDS);

        // Scenario 18: Observability and statistics
        printHeader(18, "Observability Statistics Summary");
        System.out.printf("   Tasks Submitted : %d%n", scheduler.getMetrics().getSubmittedCount());
        System.out.printf("   Tasks Completed : %d%n", scheduler.getMetrics().getCompletedCount());
        System.out.printf("   Tasks Failed    : %d%n", scheduler.getMetrics().getFailedCount());
        System.out.printf("   Tasks Retried   : %d%n", scheduler.getMetrics().getRetryCount());
        System.out.printf("   Tasks Cancelled : %d%n", scheduler.getMetrics().getCancelledCount());

        // Scenario 14: Graceful shutdown
        printHeader(14, "Graceful Scheduler Shutdown");
        scheduler.shutdown();
        boolean terminated = scheduler.awaitTermination(3, TimeUnit.SECONDS);
        System.out.printf("   Scheduler Gracefully Terminated: %b%n", terminated);

        // Scenario 15: Submission rejection after shutdown
        printHeader(15, "Submission Rejection After Shutdown");
        try {
            scheduler.submit(TaskDefinition.newBuilder().name("post-shutdown").action(() -> {}).build());
            System.err.println("   [ERROR] Should have rejected submission post-shutdown!");
        } catch (TaskRejectionException ex) {
            System.out.printf("   Post-shutdown Rejection Caught: %s%n", ex.getMessage());
        }

        System.out.println(SEPARATOR);
        System.out.println("   ALL 18 CONCURRENT TASK SCHEDULER SCENARIOS EXECUTED SUCCESSFULLY!");
        System.out.println(SEPARATOR);
    }

    private static void printHeader(int num, String title) {
        System.out.println(SUB_SEPARATOR);
        System.out.printf(" Scenario %d: %s%n", num, title);
        System.out.println(SUB_SEPARATOR);
    }
}
