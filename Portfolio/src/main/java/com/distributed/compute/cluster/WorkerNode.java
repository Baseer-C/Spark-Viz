package com.distributed.compute.cluster;

import com.distributed.compute.model.Task;
import com.distributed.compute.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a Worker Node in the distributed compute cluster.
 * 
 * A WorkerNode simulates a physical server that can execute tasks concurrently.
 * It manages a ThreadPoolExecutor with a fixed number of threads (slots) that
 * represent the node's capacity for parallel task execution.
 * 
 * Thread Safety:
 * - ThreadPoolExecutor is inherently thread-safe for task submission and execution
 * - Uses ConcurrentHashMap to track running tasks for O(1) lookup and thread-safe access
 * - Uses AtomicInteger for tracking available slots to ensure accurate concurrent updates
 * - Uses AtomicLong for tracking task execution statistics
 * - All public methods are thread-safe and can be called concurrently
 * 
 * Design Rationale:
 * - ThreadPoolExecutor chosen for task execution:
 *   * Built-in thread pool management (creation, reuse, cleanup)
 *   * Handles thread lifecycle automatically
 *   * Provides queue management for pending tasks
 *   * Supports graceful shutdown
 * - Fixed thread pool size represents the node's capacity (slots)
 * - ConcurrentHashMap for running tasks allows concurrent lookups and updates
 * - AtomicInteger for slot tracking ensures accurate availability checks without locking
 * - SynchronousQueue used as work queue to prevent task buffering (tasks are rejected if no slots available)
 * - Custom RejectedExecutionHandler provides feedback when node is at capacity
 */
public class WorkerNode {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkerNode.class);
    
    /**
     * Unique identifier for this worker node.
     * Uses UUID to ensure global uniqueness across the cluster.
     * Immutable and thread-safe.
     */
    private final String id;
    
    /**
     * Hostname or address of this worker node.
     * Used for identification and monitoring purposes.
     */
    private final String hostname;
    
    /**
     * Thread pool executor that manages task execution.
     * Fixed thread pool size represents the number of available slots on this node.
     * ThreadPoolExecutor is thread-safe for concurrent task submission.
     */
    private final ThreadPoolExecutor executor;
    
    /**
     * Map of currently running tasks by task ID.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Allows O(1) lookup of running tasks and concurrent updates.
     */
    private final ConcurrentHashMap<String, Task> runningTasks;
    
    /**
     * Atomic counter tracking the number of available slots.
     * Updated atomically when tasks are submitted or completed.
     * Ensures accurate availability checks without explicit locking.
     */
    private final AtomicInteger availableSlots;
    
    /**
     * Total number of slots (threads) available on this node.
     * Immutable and set at construction time.
     */
    private final int totalSlots;
    
    /**
     * Atomic counter tracking the total number of tasks executed by this node.
     * Thread-safe counter for monitoring and statistics.
     */
    private final AtomicLong totalTasksExecuted;
    
    /**
     * Atomic counter tracking the total execution time of all tasks.
     * Thread-safe counter for monitoring and statistics.
     */
    private final AtomicLong totalExecutionTimeMs;
    
    /**
     * Creates a new WorkerNode with the specified number of slots.
     * 
     * @param hostname The hostname or identifier for this node
     * @param slots The number of concurrent execution slots (threads)
     */
    public WorkerNode(String hostname, int slots) {
        if (slots <= 0) {
            throw new IllegalArgumentException("Number of slots must be positive");
        }
        
        this.id = UUID.randomUUID().toString();
        this.hostname = hostname != null ? hostname : "worker-" + id.substring(0, 8);
        this.totalSlots = slots;
        this.availableSlots = new AtomicInteger(slots);
        this.runningTasks = new ConcurrentHashMap<>();
        this.totalTasksExecuted = new AtomicLong(0);
        this.totalExecutionTimeMs = new AtomicLong(0);
        
        // Create thread pool with fixed size
        // SynchronousQueue ensures tasks are rejected if no threads available (no buffering)
        this.executor = new ThreadPoolExecutor(
                slots,                    // Core pool size
                slots,                    // Maximum pool size
                60L,                      // Keep-alive time (unused for fixed pool)
                TimeUnit.SECONDS,
                new SynchronousQueue<>(), // No buffering - reject if no slots available
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, hostname + "-worker-" + threadNumber.getAndIncrement());
                        t.setDaemon(false);
                        return t;
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.warn("Task rejected on {} - no available slots", hostname);
                        throw new RejectedExecutionException("Worker node " + hostname + " is at capacity");
                    }
                }
        );
        
        logger.info("WorkerNode created: {} with {} slots", hostname, slots);
    }
    
    /**
     * Gets the unique identifier of this worker node.
     * 
     * @return The node ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the hostname of this worker node.
     * 
     * @return The hostname
     */
    public String getHostname() {
        return hostname;
    }
    
    /**
     * Gets the total number of slots available on this node.
     * 
     * @return The total number of slots
     */
    public int getTotalSlots() {
        return totalSlots;
    }
    
    /**
     * Gets the number of currently available slots.
     * Thread-safe read operation.
     * 
     * @return The number of available slots
     */
    public int getAvailableSlots() {
        return availableSlots.get();
    }
    
    /**
     * Checks if this node has available slots for task execution.
     * Thread-safe operation.
     * 
     * @return true if at least one slot is available, false otherwise
     */
    public boolean hasAvailableSlots() {
        return availableSlots.get() > 0;
    }
    
    /**
     * Gets the number of currently running tasks.
     * Thread-safe operation.
     * 
     * @return The number of running tasks
     */
    public int getRunningTaskCount() {
        return runningTasks.size();
    }
    
    /**
     * Submits a task for execution on this worker node.
     * This method attempts to acquire a slot and execute the task asynchronously.
     * Thread-safe operation.
     * 
     * @param task The task to execute
     * @return A Future representing the task execution, or null if no slots available
     * @throws RejectedExecutionException if no slots are available
     */
    public Future<Void> submitTask(Task task) {
        if (task == null) {
            throw new IllegalArgumentException("Task cannot be null");
        }
        
        // Check if task can be started
        if (!task.start()) {
            logger.warn("Task {} cannot be started - invalid status", task.getId());
            throw new RejectedExecutionException("Task " + task.getId() + " cannot be started - invalid status");
        }
        
        // Decrement available slots atomically
        int currentSlots = availableSlots.getAndDecrement();
        if (currentSlots <= 0) {
            // Rollback the decrement if no slots available
            availableSlots.incrementAndGet();
            task.updateStatus(TaskStatus.RUNNING, TaskStatus.PENDING);
            throw new RejectedExecutionException("No available slots on " + hostname);
        }
        
        // Add to running tasks map
        runningTasks.put(task.getId(), task);
        
        logger.debug("Submitting task {} to worker {}", task.getId(), hostname);
        
        // Submit task to thread pool
        Future<Void> future = executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                logger.debug("Executing task {} on worker {}", task.getId(), hostname);
                
                // Execute the task (simulated work)
                task.execute();
                
                // Mark task as completed
                if (task.complete()) {
                    logger.debug("Task {} completed successfully on worker {}", task.getId(), hostname);
                } else {
                    logger.warn("Task {} could not be marked as completed", task.getId());
                }
                
                long executionTime = System.currentTimeMillis() - startTime;
                totalExecutionTimeMs.addAndGet(executionTime);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Task {} interrupted on worker {}", task.getId(), hostname);
                task.fail();
            } catch (Exception e) {
                logger.error("Task {} failed on worker {}: {}", task.getId(), hostname, e.getMessage());
                task.fail();
            } finally {
                // Remove from running tasks and increment available slots
                runningTasks.remove(task.getId());
                availableSlots.incrementAndGet();
                totalTasksExecuted.incrementAndGet();
            }
            
            return null;
        });
        
        return future;
    }
    
    /**
     * Gets a running task by its ID.
     * Thread-safe O(1) lookup operation.
     * 
     * @param taskId The ID of the task
     * @return The task if found and running, null otherwise
     */
    public Task getRunningTask(String taskId) {
        return runningTasks.get(taskId);
    }
    
    /**
     * Returns a snapshot of currently running tasks on this node.
     * Thread-safe; returns a copy of the current running tasks.
     */
    public List<Task> getRunningTasksSnapshot() {
        return new ArrayList<>(runningTasks.values());
    }
    
    /**
     * Gets the total number of tasks executed by this node.
     * Thread-safe read operation.
     * 
     * @return The total number of tasks executed
     */
    public long getTotalTasksExecuted() {
        return totalTasksExecuted.get();
    }
    
    /**
     * Gets the total execution time of all tasks in milliseconds.
     * Thread-safe read operation.
     * 
     * @return The total execution time in milliseconds
     */
    public long getTotalExecutionTimeMs() {
        return totalExecutionTimeMs.get();
    }
    
    /**
     * Gets the average execution time per task in milliseconds.
     * Thread-safe operation.
     * 
     * @return The average execution time, or 0 if no tasks executed
     */
    public double getAverageExecutionTimeMs() {
        long executed = totalTasksExecuted.get();
        if (executed == 0) {
            return 0.0;
        }
        return (double) totalExecutionTimeMs.get() / executed;
    }
    
    /**
     * Shuts down this worker node gracefully.
     * Waits for currently running tasks to complete before shutting down.
     * Thread-safe operation.
     * 
     * @param timeoutMs Maximum time to wait for shutdown in milliseconds
     * @return true if shutdown completed within timeout, false otherwise
     */
    public boolean shutdown(long timeoutMs) {
        logger.info("Shutting down worker node {}", hostname);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                logger.warn("Worker node {} shutdown forcefully after timeout", hostname);
                return false;
            }
            logger.info("Worker node {} shut down gracefully", hostname);
            return true;
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            logger.warn("Worker node {} shutdown interrupted", hostname);
            return false;
        }
    }
    
    /**
     * Shuts down this worker node immediately.
     * Attempts to interrupt running tasks.
     */
    public void shutdownNow() {
        logger.info("Shutting down worker node {} immediately", hostname);
        executor.shutdownNow();
    }
    
    @Override
    public String toString() {
        return String.format("WorkerNode{id='%s', hostname='%s', totalSlots=%d, availableSlots=%d, runningTasks=%d, totalExecuted=%d}", 
                id, hostname, totalSlots, availableSlots.get(), runningTasks.size(), totalTasksExecuted.get());
    }
}
