package com.distributed.compute.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a Stage in the DAG execution engine.
 * 
 * A Stage is a grouping of independent Tasks that can run in parallel.
 * All tasks within a stage are independent of each other and can be executed
 * concurrently across different worker nodes or threads.
 * 
 * Thread Safety:
 * - Uses ConcurrentHashMap to store tasks by ID for O(1) lookup and thread-safe operations
 * - ConcurrentHashMap provides fine-grained locking at the bucket level, allowing concurrent
 *   reads and writes without blocking, which is essential for high-throughput task management
 * - Uses AtomicInteger for tracking completion count to ensure accurate concurrent updates
 * - The tasks list is wrapped in Collections.synchronizedList for thread-safe iteration
 *   (though ConcurrentHashMap is preferred for lookups)
 * 
 * Design Rationale:
 * - ConcurrentHashMap chosen over synchronized HashMap for better concurrent performance:
 *   * Allows concurrent reads without blocking
 *   * Supports concurrent writes with minimal contention
 *   * Provides atomic operations like putIfAbsent
 * - AtomicInteger used for completion tracking to avoid synchronization overhead
 * - Stage ID is immutable (final) for safe concurrent access
 * - Tasks are stored both in a list (for iteration) and map (for O(1) lookup)
 */
public class Stage {
    
    /**
     * Unique identifier for this stage.
     * Uses UUID to ensure global uniqueness across the distributed system.
     * Immutable and thread-safe.
     */
    private final String id;
    
    /**
     * List of tasks in this stage.
     * Tasks are independent and can run in parallel.
     * Uses synchronized list wrapper for thread-safe iteration, though ConcurrentHashMap
     * is preferred for individual task lookups.
     */
    private final List<Task> tasks;
    
    /**
     * Map of tasks by their ID for O(1) lookup.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * This allows multiple threads to query task status without blocking.
     */
    private final ConcurrentHashMap<String, Task> taskMap;
    
    /**
     * Optional description of the stage for debugging and monitoring.
     */
    private final String description;
    
    /**
     * Atomic counter tracking the number of completed tasks.
     * Used to efficiently determine when all tasks in the stage are complete
     * without iterating through all tasks.
     */
    private final AtomicInteger completedTaskCount;
    
    /**
     * Creates a new Stage with the specified description.
     * 
     * @param description Optional description of the stage
     */
    public Stage(String description) {
        this.id = UUID.randomUUID().toString();
        this.tasks = Collections.synchronizedList(new ArrayList<>());
        this.taskMap = new ConcurrentHashMap<>();
        this.description = description != null ? description : "Stage-" + id.substring(0, 8);
        this.completedTaskCount = new AtomicInteger(0);
    }
    
    /**
     * Creates a new Stage with default description.
     */
    public Stage() {
        this(null);
    }
    
    /**
     * Adds a task to this stage.
     * Thread-safe operation that adds the task to both the list and map.
     * 
     * @param task The task to add
     * @return true if the task was added (always true unless task is null)
     */
    public boolean addTask(Task task) {
        if (task == null) {
            return false;
        }
        tasks.add(task);
        taskMap.put(task.getId(), task);
        return true;
    }
    
    /**
     * Adds multiple tasks to this stage.
     * Thread-safe batch operation.
     * 
     * @param tasksToAdd The tasks to add
     */
    public void addTasks(List<Task> tasksToAdd) {
        if (tasksToAdd != null) {
            for (Task task : tasksToAdd) {
                addTask(task);
            }
        }
    }
    
    /**
     * Gets a task by its ID.
     * Thread-safe O(1) lookup operation.
     * 
     * @param taskId The ID of the task to retrieve
     * @return The task if found, null otherwise
     */
    public Task getTask(String taskId) {
        return taskMap.get(taskId);
    }
    
    /**
     * Gets all tasks in this stage.
     * Returns a defensive copy to prevent external modification.
     * 
     * @return A copy of the list of tasks
     */
    public List<Task> getTasks() {
        synchronized (tasks) {
            return new ArrayList<>(tasks);
        }
    }
    
    /**
     * Gets the number of tasks in this stage.
     * Thread-safe operation.
     * 
     * @return The number of tasks
     */
    public int getTaskCount() {
        return tasks.size();
    }
    
    /**
     * Checks if all tasks in this stage are completed.
     * Efficient check using atomic counter instead of iterating through all tasks.
     * 
     * @return true if all tasks are completed, false otherwise
     */
    public boolean isCompleted() {
        return completedTaskCount.get() == tasks.size() && tasks.size() > 0;
    }
    
    /**
     * Increments the completed task counter.
     * Called when a task transitions to COMPLETED status.
     * Thread-safe atomic operation.
     */
    public void incrementCompletedCount() {
        completedTaskCount.incrementAndGet();
    }
    
    /**
     * Gets the number of completed tasks.
     * Thread-safe read operation.
     * 
     * @return The number of completed tasks
     */
    public int getCompletedTaskCount() {
        return completedTaskCount.get();
    }
    
    /**
     * Gets the unique identifier of this stage.
     * 
     * @return The stage ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the description of this stage.
     * 
     * @return The stage description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Gets the number of pending tasks in this stage.
     * Thread-safe operation that checks task statuses.
     * 
     * @return The number of pending tasks
     */
    public int getPendingTaskCount() {
        return (int) tasks.stream()
                .filter(task -> task.getStatus() == TaskStatus.PENDING)
                .count();
    }
    
    /**
     * Gets the number of running tasks in this stage.
     * Thread-safe operation that checks task statuses.
     * 
     * @return The number of running tasks
     */
    public int getRunningTaskCount() {
        return (int) tasks.stream()
                .filter(task -> task.getStatus() == TaskStatus.RUNNING)
                .count();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stage stage = (Stage) o;
        return Objects.equals(id, stage.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return String.format("Stage{id='%s', description='%s', taskCount=%d, completed=%d, isCompleted=%s}", 
                id, description, tasks.size(), completedTaskCount.get(), isCompleted());
    }
}
