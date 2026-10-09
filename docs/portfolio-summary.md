# Portfolio Summary: Concurrent Task Scheduling Engine

### One-Sentence Description
A high-throughput, thread-safe Java task scheduling library featuring priority-based dispatching, DAG dependency cycle resolution, bounded admission control, and non-blocking retry backoffs.

### Portfolio Overview (130 words)
Engineered an extensible, concurrent task scheduling library in pure Java that orchestrates background job execution across dedicated worker pools. Moving far beyond trivial wrappers around `ScheduledExecutorService`, this engine models core system programming abstractions: Directed Acyclic Graph (DAG) task dependency tracking with cycle detection, deterministic priority queues with atomic sequence number tie-breaking, and cooperative cancellation semantics. To maximize throughput, the architecture decouples execution from retry backoffs using dedicated timer threads, ensuring worker threads are never blocked by sleeping tasks. The library includes bounded queue admission control to prevent out-of-memory crashes and provides complete observability via real-time lifecycle event dispatchers. Validated across 18 multi-threaded acceptance scenarios with 87.4% test coverage.

### Technical Highlights
- **DAG Dependency Resolution**: Rejects dependency cycles at submission and triggers dependent jobs automatically upon parent completion.
- **Deterministic Priority Dispatch**: Combines priority weighting with atomic sequence counters for guaranteed FIFO execution among equal-priority jobs.
- **Worker Thread Isolation**: Unhandled exceptions and runtime errors are isolated, preventing worker thread termination.
- **Non-Blocking Exponential Retries**: Offloads retry delay countdowns to lightweight timers, freeing worker threads for immediate tasks.

### Suggested Demo / Screenshots
- Interactive CLI demonstration executing the 18 comprehensive concurrency and scheduling scenarios.
- Architecture and Sequence diagrams illustrating task dispatching and DAG execution.

### Resume Bullet Points
- Engineered a thread-safe concurrent task scheduler in Java supporting immediate, recurring, priority-ordered, and DAG-dependent task execution.
- Prevented thread starvation and memory exhaustion by decoupling retry backoffs to dedicated timer pools and enforcing bounded queue admission.
- Built a cycle-detecting dependency engine with comprehensive JUnit 5 concurrency testing achieving 87.4% test coverage with JaCoCo.
