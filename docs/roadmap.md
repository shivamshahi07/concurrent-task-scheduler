# Roadmap: Concurrent Task Scheduling Engine

## Planned Enhancements

1. **Persistent Task Store**
   - Implement JDBC/JPA backing stores for task recovery across JVM restarts.

2. **Distributed Leader Election & Worker Nodes**
   - Cluster-level scheduling using ZooKeeper or Raft consensus.

3. **Cron Expression Parser**
   - Built-in cron syntax support (`0 0 * * *`) alongside existing fixed-delay and fixed-rate modes.

4. **Dynamic Worker Auto-scaling**
   - Dynamically expand or contract worker pool threads based on real-time queue depth and CPU utilization.
