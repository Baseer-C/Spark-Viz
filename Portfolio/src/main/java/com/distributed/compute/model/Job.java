package com.distributed.compute.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a Job in the DAG execution engine.
 * 
 * A Job is a collection of Stages that must execute sequentially.
 * Stage N+1 cannot start until Stage N has completed.
 * This enforces dependencies between stages in the execution DAG.
 * 
 * Thread Safety:
 * - Uses ConcurrentHashMap to store stages by ID for O(1) lookup and thread-safe operations
 * - ConcurrentHashMap provides concurrent access without blocking, essential for monitoring
 *   and status checks from multiple threads
 * - Uses AtomicInteger for tracking completed stage count
 * - Uses AtomicReference for current stage index to ensure atomic updates when stages complete
 * - The stages list is wrapped in Collections.synchronizedList for thread-safe iteration
 * 
 * Design Rationale:
 * - ConcurrentHashMap chosen for stage lookup:
 *   * O(1) average case lookup performance
 *   * Thread-safe concurrent reads and writes
 *   * Fine-grained locking reduces contention
 * - AtomicInteger for completion tracking avoids synchronization overhead
 * - AtomicReference for current stage index ensures only one thread can advance to next stage
 * - Sequential execution enforced by checking stage completion before starting next stage
 * - Job ID is immutable (final) for safe concurrent access
 */
public class Job {
    
    /**
     * Unique identifier for this job.
     * Uses UUID to ensure global uniqueness across the distributed system.
     * Immutable and thread-safe.
     */
    private final String id;
    
    /**
     * List of stages in this job, ordered by execution sequence.
     * Stages must execute sequentially (Stage 0, then Stage 1, then Stage 2, etc.).
     * Uses synchronized list wrapper for thread-safe iteration.
     */
    private final List<Stage> stages;
    
    /**
     * Map of stages by their ID for O(1) lookup.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Allows multiple threads to query stage status without blocking.
     */
    private final ConcurrentHashMap<String, Stage> stageMap;
    
    /**
     * Optional description of the job for debugging and monitoring.
     */
    private final String description;
    
    /**
     * Atomic counter tracking the number of completed stages.
     * Used to efficiently determine job completion status.
     */
    private final AtomicInteger completedStageCount;
    
    /**
     * Atomic reference to the index of the currently executing stage.
     * Ensures atomic updates when advancing to the next stage.
     * -1 indicates no stage is currently executing.
     */
    private final AtomicReference<Integer> currentStageIndex;
    
    /**
     * Creates a new Job with the specified description.
     * 
     * @param description Optional description of the job
     */
    public Job(String description) {
        this.id = UUID.randomUUID().toString();
        this.stages = Collections.synchronizedList(new ArrayList<>());
        this.stageMap = new ConcurrentHashMap<>();
        this.description = description != null ? description : "Job-" + id.substring(0, 8);
        this.completedStageCount = new AtomicInteger(0);
        this.currentStageIndex = new AtomicReference<>(-1);
    }
    
    /**
     * Creates a new Job with default description.
     */
    public Job() {
        this(null);
    }
    
    /**
     * Adds a stage to this job.
     * Stages are added in order and will execute sequentially.
     * Thread-safe operation that adds the stage to both the list and map.
     * 
     * @param stage The stage to add
     * @return true if the stage was added (always true unless stage is null)
     */
    public boolean addStage(Stage stage) {
        if (stage == null) {
            return false;
        }
        stages.add(stage);
        stageMap.put(stage.getId(), stage);
        return true;
    }
    
    /**
     * Adds multiple stages to this job.
     * Stages are added in order and will execute sequentially.
     * Thread-safe batch operation.
     * 
     * @param stagesToAdd The stages to add
     */
    public void addStages(List<Stage> stagesToAdd) {
        if (stagesToAdd != null) {
            for (Stage stage : stagesToAdd) {
                addStage(stage);
            }
        }
    }
    
    /**
     * Gets a stage by its ID.
     * Thread-safe O(1) lookup operation.
     * 
     * @param stageId The ID of the stage to retrieve
     * @return The stage if found, null otherwise
     */
    public Stage getStage(String stageId) {
        return stageMap.get(stageId);
    }
    
    /**
     * Gets a stage by its index in the execution sequence.
     * Thread-safe operation.
     * 
     * @param index The index of the stage (0-based)
     * @return The stage at the specified index, or null if index is out of bounds
     */
    public Stage getStage(int index) {
        if (index < 0 || index >= stages.size()) {
            return null;
        }
        synchronized (stages) {
            return stages.get(index);
        }
    }
    
    /**
     * Gets all stages in this job.
     * Returns a defensive copy to prevent external modification.
     * 
     * @return A copy of the list of stages
     */
    public List<Stage> getStages() {
        synchronized (stages) {
            return new ArrayList<>(stages);
        }
    }
    
    /**
     * Gets the number of stages in this job.
     * Thread-safe operation.
     * 
     * @return The number of stages
     */
    public int getStageCount() {
        return stages.size();
    }
    
    /**
     * Gets the index of the currently executing stage.
     * Returns -1 if no stage is currently executing.
     * Thread-safe read operation.
     * 
     * @return The current stage index, or -1 if none
     */
    public int getCurrentStageIndex() {
        return currentStageIndex.get();
    }
    
    /**
     * Gets the currently executing stage.
     * Thread-safe operation.
     * 
     * @return The current stage, or null if no stage is executing
     */
    public Stage getCurrentStage() {
        int index = currentStageIndex.get();
        return getStage(index);
    }
    
    /**
     * Attempts to start the first stage (index 0) if no stage is currently executing.
     * Thread-safe operation using compare-and-set semantics.
     * 
     * @return true if the first stage was successfully started, false otherwise
     */
    public boolean startFirstStage() {
        if (stages.isEmpty()) {
            return false;
        }
        return currentStageIndex.compareAndSet(-1, 0);
    }
    
    /**
     * Attempts to advance to the next stage after the current stage completes.
     * This method atomically increments the current stage index if the previous stage
     * has completed and there are more stages to execute.
     * Thread-safe operation using compare-and-set semantics.
     * 
     * @return true if successfully advanced to next stage, false if job is complete or already at last stage
     */
    public boolean advanceToNextStage() {
        int currentIndex = currentStageIndex.get();
        
        // Check if we're at the last stage
        if (currentIndex >= stages.size() - 1) {
            return false;
        }
        
        // Verify current stage is completed before advancing
        Stage currentStage = getStage(currentIndex);
        if (currentStage == null || !currentStage.isCompleted()) {
            return false;
        }
        
        // Atomically advance to next stage
        int nextIndex = currentIndex + 1;
        boolean advanced = currentStageIndex.compareAndSet(currentIndex, nextIndex);
        
        if (advanced) {
            completedStageCount.incrementAndGet();
        }
        
        return advanced;
    }
    
    /**
     * Marks the current stage as completed and advances to the next stage if available.
     * Thread-safe operation.
     * 
     * @return true if advanced to next stage, false if job is complete
     */
    public boolean completeCurrentStage() {
        int currentIndex = currentStageIndex.get();
        if (currentIndex < 0) {
            return false;
        }
        
        Stage currentStage = getStage(currentIndex);
        if (currentStage != null && currentStage.isCompleted()) {
            return advanceToNextStage();
        }
        
        return false;
    }
    
    /**
     * Checks if the job is completed (all stages have completed).
     * Thread-safe operation.
     * 
     * @return true if all stages are completed, false otherwise
     */
    public boolean isCompleted() {
        int currentIndex = currentStageIndex.get();
        if (currentIndex < 0 || stages.isEmpty()) {
            return false;
        }
        
        // Check if we've completed the last stage
        if (currentIndex >= stages.size() - 1) {
            Stage lastStage = getStage(currentIndex);
            return lastStage != null && lastStage.isCompleted();
        }
        
        return false;
    }
    
    /**
     * Gets the number of completed stages.
     * Thread-safe read operation.
     * 
     * @return The number of completed stages
     */
    public int getCompletedStageCount() {
        return completedStageCount.get();
    }
    
    /**
     * Gets the unique identifier of this job.
     * 
     * @return The job ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the description of this job.
     * 
     * @return The job description
     */
    public String getDescription() {
        return description;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return String.format("Job{id='%s', description='%s', stageCount=%d, completedStages=%d, currentStageIndex=%d, isCompleted=%s}", 
                id, description, stages.size(), completedStageCount.get(), currentStageIndex.get(), isCompleted());
    }
}
