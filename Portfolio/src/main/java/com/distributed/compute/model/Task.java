package com.distributed.compute.model;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a unit of work in the distributed compute engine.
 * 
 * A Task is the fundamental execution unit that performs a specific computation.
 * Each task has:
 * - A unique identifier (UUID)
 * - A status that tracks its lifecycle (PENDING -> RUNNING -> COMPLETED/FAILED)
 * - A duration that simulates the work to be performed
 * 
 * Thread Safety:
 * - Uses AtomicReference for status to ensure thread-safe status updates without explicit locking
 * - AtomicReference provides compare-and-set semantics, preventing race conditions when multiple
 *   threads attempt to update the status simultaneously
 * - The task ID is immutable (final), ensuring safe concurrent access
 * - Duration is immutable (final), set at construction time
 * 
 * Design Rationale:
 * - AtomicReference chosen over synchronized blocks for better performance in high-concurrency scenarios
 * - Status transitions are atomic operations, ensuring consistency
 * - Immutable fields (id, duration) reduce synchronization needs
 */
public class Task {
    
    /**
     * Unique identifier for this task.
     * Uses UUID to ensure global uniqueness across the distributed system.
     * Immutable and thread-safe.
     */
    private final String id;
    
    /**
     * Current status of the task.
     * Uses AtomicReference to ensure thread-safe status updates.
     * Multiple threads may attempt to update status (e.g., scheduler assigning task,
     * worker completing task), so atomic operations are essential.
     */
    private final AtomicReference<TaskStatus> status;
    
    /**
     * Duration in milliseconds that this task will take to execute.
     * Simulated via Thread.sleep() during execution.
     * Immutable and set at construction time.
     */
    private final long durationMs;
    
    /**
     * Optional description of the task for debugging and monitoring purposes.
     */
    private final String description;
    
    /**
     * Memory in MB this task uses while running (simulated).
     * Used for scheduling: a worker must have at least this much free memory to run the task.
     */
    private final int memoryMb;
    
    /**
     * Creates a new Task with the specified duration, description, and memory.
     *
     * @param durationMs The duration in milliseconds this task will take to execute
     * @param description Optional description of the task
     * @param memoryMb Memory in MB the task uses while running (0 = no memory constraint)
     */
    public Task(long durationMs, String description, int memoryMb) {
        this.id = UUID.randomUUID().toString();
        this.status = new AtomicReference<>(TaskStatus.PENDING);
        this.durationMs = durationMs;
        this.description = description != null ? description : "Task-" + id.substring(0, 8);
        this.memoryMb = memoryMb >= 0 ? memoryMb : 0;
    }
    
    /**
     * Creates a new Task with the specified duration and description (no memory).
     */
    public Task(long durationMs, String description) {
        this(durationMs, description, 0);
    }
    
    /**
     * Creates a new Task with the specified duration and default description.
     */
    public Task(long durationMs) {
        this(durationMs, null, 0);
    }
    
    /**
     * Gets the unique identifier of this task.
     * 
     * @return The task ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the current status of this task.
     * Thread-safe read operation.
     * 
     * @return The current task status
     */
    public TaskStatus getStatus() {
        return status.get();
    }
    
    /**
     * Atomically updates the task status from the expected value to the new value.
     * This ensures that status transitions are atomic and prevents race conditions.
     * 
     * @param expectedStatus The expected current status
     * @param newStatus The new status to set
     * @return true if the update was successful (status matched expected), false otherwise
     */
    public boolean updateStatus(TaskStatus expectedStatus, TaskStatus newStatus) {
        return status.compareAndSet(expectedStatus, newStatus);
    }
    
    /**
     * Sets the status to RUNNING if currently PENDING.
     * Thread-safe operation using compare-and-set semantics.
     * 
     * @return true if status was successfully changed to RUNNING, false if already in another state
     */
    public boolean start() {
        return updateStatus(TaskStatus.PENDING, TaskStatus.RUNNING);
    }
    
    /**
     * Sets the status to COMPLETED if currently RUNNING.
     * Thread-safe operation using compare-and-set semantics.
     * 
     * @return true if status was successfully changed to COMPLETED, false if not in RUNNING state
     */
    public boolean complete() {
        return updateStatus(TaskStatus.RUNNING, TaskStatus.COMPLETED);
    }
    
    /**
     * Sets the status to FAILED if currently RUNNING.
     * Thread-safe operation using compare-and-set semantics.
     * 
     * @return true if status was successfully changed to FAILED, false if not in RUNNING state
     */
    public boolean fail() {
        return updateStatus(TaskStatus.RUNNING, TaskStatus.FAILED);
    }
    
    /**
     * Gets the duration in milliseconds this task will take to execute.
     * 
     * @return The task duration in milliseconds
     */
    public long getDurationMs() {
        return durationMs;
    }
    
    /**
     * Gets the description of this task.
     *
     * @return The task description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Gets the memory in MB this task uses while running.
     *
     * @return Memory in MB (0 = no constraint)
     */
    public int getMemoryMb() {
        return memoryMb;
    }
    
    /**
     * Executes this task by sleeping for the specified duration.
     * This simulates the actual work performed by the task.
     * 
     * @throws InterruptedException if the thread is interrupted during execution
     */
    public void execute() throws InterruptedException {
        Thread.sleep(durationMs);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equals(id, task.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return String.format("Task{id='%s', status=%s, durationMs=%d, memoryMb=%d, description='%s'}", 
                id, status.get(), durationMs, memoryMb, description);
    }
}
