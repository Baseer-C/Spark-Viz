package com.distributed.compute.cluster;

import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import com.distributed.compute.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The ClusterManager is the central coordinator ("brain") of the distributed compute engine.
 * 
 * It manages the state of all WorkerNodes and assigns Tasks to available slots across the cluster.
 * The ClusterManager coordinates job execution by:
 * 1. Tracking all worker nodes and their availability
 * 2. Scheduling tasks from stages to available worker slots
 * 3. Monitoring task completion and advancing stages
 * 4. Managing job lifecycle (submission, execution, completion)
 * 
 * Thread Safety:
 * - Uses ConcurrentHashMap to store worker nodes by ID for thread-safe concurrent access
 * - ConcurrentHashMap provides fine-grained locking, allowing concurrent reads and writes
 * - Uses ConcurrentHashMap to store jobs by ID for thread-safe job management
 * - Uses synchronized collections for task queues to ensure thread-safe scheduling
 * - All public methods are thread-safe and can be called concurrently
 * - Uses AtomicInteger for tracking statistics
 * 
 * Design Rationale:
 * - ConcurrentHashMap chosen for worker and job storage:
 *   * O(1) average case lookup performance
 *   * Thread-safe concurrent reads without blocking
 *   * Supports concurrent writes with minimal contention
 *   * Fine-grained locking at bucket level reduces lock contention
 * - ScheduledExecutorService for periodic task scheduling and monitoring
 * - Synchronized collections for task queues ensure thread-safe scheduling operations
 * - Worker selection uses round-robin with availability check for load balancing
 * - Job execution is coordinated through stage completion monitoring
 */
@Component
public class ClusterManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    /**
     * Map of all registered worker nodes by their ID.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Multiple threads may query or update worker state simultaneously.
     */
    private final ConcurrentHashMap<String, WorkerNode> workerNodes;
    
    /**
     * Map of all submitted jobs by their ID.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Allows concurrent job submission and status queries.
     */
    private final ConcurrentHashMap<String, Job> jobs;
    
    /**
     * Queue of pending tasks waiting to be scheduled.
     * Uses synchronized queue for thread-safe task submission and retrieval.
     * Tasks are added when stages become ready and removed when scheduled to workers.
     */
    private final Queue<Task> pendingTaskQueue;
    
    /**
     * Map of tasks to their associated stages for efficient stage completion tracking.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Allows O(1) lookup of stage when task completes.
     */
    private final ConcurrentHashMap<String, Stage> taskToStageMap;
    
    /**
     * Map of stages to their associated jobs for efficient job completion tracking.
     * Uses ConcurrentHashMap for thread-safe concurrent access.
     * Allows O(1) lookup of job when stage completes.
     */
    private final ConcurrentHashMap<String, Job> stageToJobMap;
    
    /**
     * Set of stage IDs that have had their tasks queued.
     * Prevents re-queuing tasks when job execution is retried or interrupted.
     * Uses ConcurrentHashMap as a ConcurrentSet for thread-safe operations.
     */
    private final ConcurrentHashMap<String, Boolean> queuedStages;
    
    /**
     * Scheduled executor service for periodic task scheduling and monitoring.
     * Runs background threads to schedule pending tasks and check job/stage completion.
     */
    private ScheduledExecutorService scheduler;
    
    /**
     * Executor service for asynchronous job execution.
     * Handles job lifecycle management and stage coordination.
     */
    private ExecutorService jobExecutor;
    
    /**
     * Atomic counter for round-robin worker selection.
     * Ensures fair distribution of tasks across workers.
     */
    private final AtomicInteger workerSelectionIndex;
    
    /**
     * Atomic counter tracking the total number of tasks scheduled.
     * Thread-safe counter for monitoring and statistics.
     */
    private final AtomicInteger totalTasksScheduled;
    
    /**
     * Atomic counter tracking the total number of jobs submitted.
     * Thread-safe counter for monitoring and statistics.
     */
    private final AtomicInteger totalJobsSubmitted;
    
    /**
     * Scheduling interval in milliseconds.
     * How often the scheduler checks for pending tasks and available slots.
     */
    private static final long SCHEDULING_INTERVAL_MS = 100;
    
    /**
     * Job IDs for which we have already logged completion.
     * Prevents spamming the log with the same completed job every scheduler tick.
     */
    private final Set<String> completedJobsLogged;
    
    /**
     * Creates a new ClusterManager.
     * Initializes all data structures and starts the scheduling service.
     */
    public ClusterManager() {
        this.workerNodes = new ConcurrentHashMap<>();
        this.jobs = new ConcurrentHashMap<>();
        this.pendingTaskQueue = new ConcurrentLinkedQueue<>();
        this.taskToStageMap = new ConcurrentHashMap<>();
        this.stageToJobMap = new ConcurrentHashMap<>();
        this.queuedStages = new ConcurrentHashMap<>();
        this.workerSelectionIndex = new AtomicInteger(0);
        this.totalTasksScheduled = new AtomicInteger(0);
        this.totalJobsSubmitted = new AtomicInteger(0);
        this.completedJobsLogged = ConcurrentHashMap.newKeySet();
    }
    
    /**
     * Initializes the ClusterManager after Spring context is loaded.
     * Starts the scheduler and job executor services.
     */
    @PostConstruct
    public void initialize() {
        logger.info("Initializing ClusterManager");
        
        // Create scheduler for periodic task scheduling
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "cluster-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // Create executor for job management
        this.jobExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "job-executor");
            t.setDaemon(true);
            return t;
        });
        
        // Start periodic task scheduling
        scheduler.scheduleAtFixedRate(
                this::schedulePendingTasks,
                0,
                SCHEDULING_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        
        // Start periodic job/stage completion monitoring
        scheduler.scheduleAtFixedRate(
                this::monitorJobProgress,
                0,
                SCHEDULING_INTERVAL_MS * 2,
                TimeUnit.MILLISECONDS
        );
        
        logger.info("ClusterManager initialized");
    }
    
    /**
     * Shuts down the ClusterManager gracefully.
     * Stops all services and shuts down worker nodes.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ClusterManager");
        
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (jobExecutor != null) {
            jobExecutor.shutdown();
            try {
                if (!jobExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    jobExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                jobExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Shutdown all worker nodes
        for (WorkerNode worker : workerNodes.values()) {
            worker.shutdown(5000);
        }
        
        logger.info("ClusterManager shut down");
    }
    
    /**
     * Registers a new worker node with the cluster.
     * Thread-safe operation.
     * 
     * @param workerNode The worker node to register
     * @return true if the worker was registered, false if already exists
     */
    public boolean registerWorker(WorkerNode workerNode) {
        if (workerNode == null) {
            throw new IllegalArgumentException("Worker node cannot be null");
        }
        
        WorkerNode existing = workerNodes.putIfAbsent(workerNode.getId(), workerNode);
        if (existing == null) {
            logger.info("Registered worker node: {}", workerNode.getHostname());
            return true;
        } else {
            logger.warn("Worker node {} already registered", workerNode.getHostname());
            return false;
        }
    }
    
    /**
     * Removes a worker node from the cluster.
     * Thread-safe operation.
     * 
     * @param workerId The ID of the worker node to remove
     * @return true if the worker was removed, false if not found
     */
    public boolean removeWorker(String workerId) {
        WorkerNode worker = workerNodes.remove(workerId);
        if (worker != null) {
            logger.info("Removed worker node: {}", worker.getHostname());
            worker.shutdown(5000);
            return true;
        }
        return false;
    }
    
    /**
     * Gets a worker node by its ID.
     * Thread-safe O(1) lookup operation.
     * 
     * @param workerId The ID of the worker node
     * @return The worker node if found, null otherwise
     */
    public WorkerNode getWorker(String workerId) {
        return workerNodes.get(workerId);
    }
    
    /**
     * Gets all registered worker nodes.
     * Returns a defensive copy to prevent external modification.
     * 
     * @return A list of all worker nodes
     */
    public List<WorkerNode> getAllWorkers() {
        return new ArrayList<>(workerNodes.values());
    }
    
    /**
     * Gets the total number of available slots across all worker nodes.
     * Thread-safe operation.
     * 
     * @return The total number of available slots
     */
    public int getTotalAvailableSlots() {
        return workerNodes.values().stream()
                .mapToInt(WorkerNode::getAvailableSlots)
                .sum();
    }
    
    /**
     * Submits a job for execution in the cluster.
     * Thread-safe operation that queues the job and starts execution.
     * 
     * @param job The job to submit
     * @return true if the job was submitted successfully
     */
    public boolean submitJob(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null");
        }
        
        Job existing = jobs.putIfAbsent(job.getId(), job);
        if (existing != null) {
            logger.warn("Job {} already submitted", job.getId());
            return false;
        }
        
        totalJobsSubmitted.incrementAndGet();
        logger.info("Submitted job: {} with {} stages", job.getId(), job.getStageCount());
        
        // Start job execution asynchronously
        jobExecutor.submit(() -> executeJob(job));
        
        return true;
    }
    
    /**
     * Executes a job by coordinating stage execution.
     * Stages execute sequentially, tasks within stages execute in parallel.
     * 
     * @param job The job to execute
     */
    private void executeJob(Job job) {
        logger.info("Starting execution of job: {}", job.getId());
        
        // Start the first stage
        if (!job.startFirstStage()) {
            logger.warn("Job {} has no stages to execute", job.getId());
            return;
        }
        
        // Process stages sequentially
        while (!job.isCompleted() && !Thread.currentThread().isInterrupted()) {
            Stage currentStage = job.getCurrentStage();
            if (currentStage == null) {
                break;
            }
            
            // Check if stage tasks have already been queued (prevent re-queuing)
            if (!queuedStages.containsKey(currentStage.getId())) {
                logger.info("Starting stage {} of job {}", currentStage.getId(), job.getId());
                
                // Mark stage as queued
                queuedStages.put(currentStage.getId(), Boolean.TRUE);
                
                // Queue all tasks from current stage
                for (Task task : currentStage.getTasks()) {
                    // Only queue if task is still pending
                    if (task.getStatus() == TaskStatus.PENDING) {
                        pendingTaskQueue.offer(task);
                        taskToStageMap.put(task.getId(), currentStage);
                        stageToJobMap.put(currentStage.getId(), job);
                    }
                }
            }
            
            // Wait for current stage to complete
            try {
                waitForStageCompletion(currentStage);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Job {} execution interrupted", job.getId());
                return;
            }
            
            // Check if interrupted before advancing
            if (Thread.currentThread().isInterrupted()) {
                logger.warn("Job {} execution interrupted", job.getId());
                return;
            }
            
            // Advance to next stage
            if (job.completeCurrentStage()) {
                logger.info("Stage {} completed, advancing to next stage", currentStage.getId());
            } else if (job.isCompleted()) {
                logger.info("Job {} completed successfully", job.getId());
            }
        }
        
        if (Thread.currentThread().isInterrupted()) {
            logger.warn("Job {} execution was interrupted", job.getId());
        }
    }
    
    /**
     * Waits for a stage to complete by monitoring task completion.
     * 
     * @param stage The stage to wait for
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void waitForStageCompletion(Stage stage) throws InterruptedException {
        while (!stage.isCompleted()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted while waiting for stage completion");
            }
            Thread.sleep(SCHEDULING_INTERVAL_MS);
        }
    }
    
    /**
     * Periodically schedules pending tasks to available worker slots.
     * This method is called by the scheduler at regular intervals.
     */
    private void schedulePendingTasks() {
        if (pendingTaskQueue.isEmpty() || workerNodes.isEmpty()) {
            return;
        }
        
        List<WorkerNode> availableWorkers = workerNodes.values().stream()
                .filter(WorkerNode::hasAvailableSlots)
                .collect(Collectors.toList());
        
        if (availableWorkers.isEmpty()) {
            return;
        }
        
        // Schedule tasks to available workers using round-robin
        int scheduled = 0;
        while (!pendingTaskQueue.isEmpty() && !availableWorkers.isEmpty()) {
            Task task = pendingTaskQueue.poll();
            if (task == null || task.getStatus() != TaskStatus.PENDING) {
                continue;
            }
            
            WorkerNode worker = selectWorker(availableWorkers);
            if (worker == null) {
                // No available workers, re-queue task
                pendingTaskQueue.offer(task);
                break;
            }
            
            try {
                Future<Void> future = worker.submitTask(task);
                if (future != null) {
                    scheduled++;
                    totalTasksScheduled.incrementAndGet();
                    
                    // Monitor task completion asynchronously
                    jobExecutor.submit(() -> {
                        try {
                            future.get();
                            handleTaskCompletion(task);
                        } catch (Exception e) {
                            logger.error("Error monitoring task {}: {}", task.getId(), e.getMessage());
                        }
                    });
                } else {
                    // Re-queue task if submission failed
                    pendingTaskQueue.offer(task);
                }
            } catch (RejectedExecutionException e) {
                // Worker became unavailable, re-queue task
                pendingTaskQueue.offer(task);
            }
            
            // Refresh available workers list
            availableWorkers = workerNodes.values().stream()
                    .filter(WorkerNode::hasAvailableSlots)
                    .collect(Collectors.toList());
        }
        
        if (scheduled > 0) {
            logger.debug("Scheduled {} tasks to workers", scheduled);
        }
    }
    
    /**
     * Selects a worker node using round-robin selection.
     * Ensures fair distribution of tasks across workers.
     * 
     * @param availableWorkers List of workers with available slots
     * @return A worker node with available slots, or null if none available
     */
    private WorkerNode selectWorker(List<WorkerNode> availableWorkers) {
        if (availableWorkers.isEmpty()) {
            return null;
        }
        
        int index = workerSelectionIndex.getAndIncrement() % availableWorkers.size();
        WorkerNode selected = availableWorkers.get(index);
        
        // Verify worker still has slots (may have been taken by another thread)
        if (selected.hasAvailableSlots()) {
            return selected;
        }
        
        // Try next available worker
        for (WorkerNode worker : availableWorkers) {
            if (worker.hasAvailableSlots()) {
                return worker;
            }
        }
        
        return null;
    }
    
    /**
     * Handles task completion by updating stage and job state.
     * 
     * @param task The completed task
     */
    private void handleTaskCompletion(Task task) {
        Stage stage = taskToStageMap.get(task.getId());
        if (stage != null) {
            stage.incrementCompletedCount();
        }
    }
    
    /**
     * Periodically monitors job and stage progress.
     * Logs each job's completion only once to avoid log spam.
     */
    private void monitorJobProgress() {
        for (Job job : jobs.values()) {
            if (job.isCompleted() && completedJobsLogged.add(job.getId())) {
                logger.info("Job {} completed: {}", job.getId(), job);
            }
        }
    }
    
    /**
     * Gets a job by its ID.
     * Thread-safe O(1) lookup operation.
     * 
     * @param jobId The ID of the job
     * @return The job if found, null otherwise
     */
    public Job getJob(String jobId) {
        return jobs.get(jobId);
    }
    
    /**
     * Gets all submitted jobs.
     * Returns a defensive copy to prevent external modification.
     * 
     * @return A list of all jobs
     */
    public List<Job> getAllJobs() {
        return new ArrayList<>(jobs.values());
    }
    
    /**
     * Gets the total number of tasks scheduled.
     * Thread-safe read operation.
     * 
     * @return The total number of tasks scheduled
     */
    public int getTotalTasksScheduled() {
        return totalTasksScheduled.get();
    }
    
    /**
     * Gets the total number of jobs submitted.
     * Thread-safe read operation.
     * 
     * @return The total number of jobs submitted
     */
    public int getTotalJobsSubmitted() {
        return totalJobsSubmitted.get();
    }
    
    /**
     * Gets cluster statistics.
     * Thread-safe operation.
     * 
     * @return A map containing cluster statistics
     */
    public Map<String, Object> getClusterStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("workerCount", workerNodes.size());
        stats.put("totalSlots", workerNodes.values().stream().mapToInt(WorkerNode::getTotalSlots).sum());
        stats.put("availableSlots", getTotalAvailableSlots());
        stats.put("runningTasks", workerNodes.values().stream().mapToInt(WorkerNode::getRunningTaskCount).sum());
        stats.put("pendingTasks", pendingTaskQueue.size());
        stats.put("totalJobs", jobs.size());
        stats.put("totalTasksScheduled", totalTasksScheduled.get());
        stats.put("totalJobsSubmitted", totalJobsSubmitted.get());
        return stats;
    }
    
    @Override
    public String toString() {
        return String.format("ClusterManager{workers=%d, jobs=%d, pendingTasks=%d, availableSlots=%d}", 
                workerNodes.size(), jobs.size(), pendingTaskQueue.size(), getTotalAvailableSlots());
    }
}
