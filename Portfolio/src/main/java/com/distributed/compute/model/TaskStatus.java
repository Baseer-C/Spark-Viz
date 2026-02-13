package com.distributed.compute.model;

/**
 * Enumeration representing the lifecycle states of a Task in the distributed compute engine.
 * 
 * The state machine follows this progression:
 * PENDING -> RUNNING -> (COMPLETED | FAILED)
 * 
 * This enum is thread-safe as enum instances are inherently immutable and thread-safe.
 */
public enum TaskStatus {
    /**
     * Task has been created but not yet assigned to a worker node.
     * This is the initial state for all tasks.
     */
    PENDING,
    
    /**
     * Task is currently being executed on a worker node.
     * The task has been assigned to a thread slot and is actively running.
     */
    RUNNING,
    
    /**
     * Task has completed successfully.
     * The task execution finished without errors.
     */
    COMPLETED,
    
    /**
     * Task execution failed due to an error or exception.
     * The task encountered an error during execution and could not complete.
     */
    FAILED
}
