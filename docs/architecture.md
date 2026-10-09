# Architecture Overview: Concurrent Task Scheduling Engine

## 1. Architectural Layers

The Concurrent Task Scheduling Engine separates task specification, priority dispatching, execution lifecycle, and dependency resolution:

```
[ Client / Demo Runner ]
           │
           ▼
   [ TaskScheduler API (Facade) ]
    ├── [ DependencyGraph (DAG Cycle Detector) ]
    ├── [ TaskDispatchQueue (Priority + Tie-Breaking FIFO) ]
    ├── [ EventDispatcher (Lifecycle Observer) ]
    └── [ WorkerPool ]
          ├── [ Worker Threads (Named) ]
          └── [ Timer Executor (Non-blocking backoff & delays) ]
```

### Components
1. **TaskScheduler (Facade)**: Single point of contact for clients. Validates DAG dependencies, checks scheduler shutdown status, and routes immediate or delayed jobs.
2. **DependencyGraph (DAG)**: Manages prerequisites using an adjacency graph. Executes breadth-first cycle detection upon each task submission to eliminate deadlock risks before tasks reach the queue.
3. **TaskDispatchQueue**: Wraps a `PriorityBlockingQueue<PrioritizedTaskItem>`. Attaches atomic sequence numbers to every job to guarantee deterministic FIFO tie-breaking among equal-priority tasks.
4. **WorkerPool**: Manages dedicated daemon worker threads (`scheduler-worker-N`). Dispatches tasks, guards lifecycle transitions via `TaskRuntimeContext`, captures errors without worker thread termination, and delegates backoff sleeps to a lightweight timer pool (`scheduler-timer`).
5. **SchedulerMetrics & EventDispatcher**: Provides decoupled telemetry and lifecycle notifications (`TASK_SUBMITTED`, `TASK_STARTED`, `TASK_COMPLETED`, `TASK_FAILED`, `RETRY_SCHEDULED`, `TASK_CANCELLED`).
