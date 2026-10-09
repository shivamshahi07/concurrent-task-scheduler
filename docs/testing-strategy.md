# Testing Strategy: Concurrent Task Scheduling Engine

## 1. Overview
The test suite ensures deadlock-free, race-free, and high-performance task execution under concurrent loads.

| Test Class | Focus |
|---|---|
| `TaskSchedulerTest` | Core acceptance: immediate, delayed, recurring tasks, priority queues, retry backoffs, DAG dependencies, cycle detection, bounded admission, worker fault isolation. |
| `DomainAndPolicyTest` | `JobId` formatting, `JobStatus` state transitions, `Priority` levels, `ExponentialBackoffRetryPolicy` fatal exception filters, `TaskDefinition.Builder`. |
| `CoreComponentsTest` | `TaskDispatchQueue` poll/offer/remove, `TaskRuntimeContext` atomic transitions, `SchedulerMetrics`, `TaskSchedulerEvent` records, `EventDispatcher` error boundary. |

## 2. Concurrency & Determinism
- Concurrency tests utilize `CountDownLatch` and `AtomicBoolean` rather than arbitrary long thread sleeps.
- Priority ordering is validated deterministically by holding a worker thread with an active latch while saturating the priority queue.
- Cycles in dependencies are verified to fail fast at the point of submission.
