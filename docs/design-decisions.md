# Design Decisions: Concurrent Task Scheduling Engine

## 1. Design Patterns Implemented

| Pattern | Implementation | Problem Solved |
|---|---|---|
| **Command** | `TaskDefinition` wrapping `Runnable` / `Callable` | Encapsulates executable actions and metadata into an immutable object passed across queues and threads. |
| **Builder** | `TaskDefinition.newBuilder()` | Provides readable, flexible construction of tasks with optional delays, priorities, retries, and dependencies. |
| **Strategy** | `RetryPolicy`, `ExponentialBackoffRetryPolicy` | Pluggable retry mechanisms allowing different exponential backoffs, jitter, or fatal exception filters. |
| **Observer** | `EventDispatcher`, `TaskLifecycleListener` | Enables monitoring, logging, and metrics without tight coupling to worker execution loops. |
| **Facade** | `TaskScheduler` | Provides a simple, cohesive API while encapsulating complex multithreaded queues, worker threads, and DAG state. |

---

## 2. Alternatives Considered & Trade-Offs

### Custom Engine vs Wrapping `ScheduledExecutorService`
- **Decision**: Implemented priority dispatch, task dependency DAGs, bounded admission, and lifecycle state machines from first principles.
- **Alternative**: Merely calling `ScheduledThreadPoolExecutor`.
- **Reasoning**: `ScheduledThreadPoolExecutor` cannot prioritize tasks by custom domain priorities, does not support task dependency chains without blocking worker threads, cannot apply exponential backoff retries without thread-sleep, and lacks explicit granular state machines.

### Dedicated Timer vs Worker Sleep
- **Decision**: Backoff retries and delayed executions are scheduled via a dedicated 2-thread `ScheduledExecutorService` timer pool.
- **Alternative**: Calling `Thread.sleep()` inside the worker thread during backoff.
- **Reasoning**: Sleeping worker threads monopolizes worker capacity, starving other eligible high-priority tasks. Delegating delays to a timer pool frees the worker immediately.

### Deterministic FIFO Tie-Breaking
- **Decision**: Added atomic sequence numbers to `PrioritizedTaskItem`.
- **Alternative**: Relying on arbitrary heap order in `PriorityBlockingQueue` for equal priority tasks.
- **Reasoning**: Unordered heaps cause unpredictable starvation for equal priority tasks. Monotonically increasing sequence counters ensure exact FIFO order among equal-priority jobs.
