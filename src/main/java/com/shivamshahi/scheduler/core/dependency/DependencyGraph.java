package com.shivamshahi.scheduler.core.dependency;

import com.shivamshahi.scheduler.domain.exception.CyclicDependencyException;
import com.shivamshahi.scheduler.domain.model.JobId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Directed Acyclic Graph (DAG) for managing task prerequisites and dependency cycles.
 */
public class DependencyGraph {
    // child -> set of parent dependencies that child must wait for
    private final Map<JobId, Set<JobId>> prerequisites = new ConcurrentHashMap<>();
    // parent -> set of downstream children dependent on parent
    private final Map<JobId, Set<JobId>> dependents = new ConcurrentHashMap<>();

    public synchronized void registerDependencies(JobId task, Set<JobId> dependencies) {
        Objects.requireNonNull(task, "Task ID cannot be null");
        if (dependencies == null || dependencies.isEmpty()) {
            return;
        }

        // Cycle check: can any dependency reach task?
        for (JobId dep : dependencies) {
            if (dep.equals(task)) {
                throw new CyclicDependencyException("Task cannot depend on itself: " + task.value());
            }
            if (hasPath(dep, task)) {
                throw new CyclicDependencyException(
                        String.format("Cyclic dependency detected between task %s and %s", task.value(), dep.value())
                );
            }
        }

        prerequisites.computeIfAbsent(task, k -> new CopyOnWriteArraySet<>()).addAll(dependencies);
        for (JobId dep : dependencies) {
            dependents.computeIfAbsent(dep, k -> new CopyOnWriteArraySet<>()).add(task);
        }
    }

    private boolean hasPath(JobId start, JobId target) {
        Set<JobId> visited = new HashSet<>();
        Queue<JobId> queue = new ArrayDeque<>();
        queue.add(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            JobId current = queue.poll();
            Set<JobId> currentPrereqs = prerequisites.get(current);
            if (currentPrereqs != null) {
                for (JobId next : currentPrereqs) {
                    if (next.equals(target)) {
                        return true;
                    }
                    if (visited.add(next)) {
                        queue.add(next);
                    }
                }
            }
        }
        return false;
    }

    public synchronized Set<JobId> getPrerequisites(JobId task) {
        Set<JobId> set = prerequisites.get(task);
        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    public synchronized Set<JobId> getDependents(JobId parent) {
        Set<JobId> set = dependents.get(parent);
        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    public synchronized void removeCompletedDependency(JobId parent, JobId child) {
        Set<JobId> set = prerequisites.get(child);
        if (set != null) {
            set.remove(parent);
            if (set.isEmpty()) {
                prerequisites.remove(child);
            }
        }
    }
}
