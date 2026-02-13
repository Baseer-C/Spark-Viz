package com.distributed.compute.demo;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.cluster.WorkerNode;
import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Demo class to demonstrate the distributed compute engine.
 * This class runs automatically when the Spring Boot application starts.
 * 
 * To run: mvn spring-boot:run
 * To disable during tests: set demo.enabled=false in application.properties
 */
@Component
@ConditionalOnProperty(name = "demo.enabled", havingValue = "true", matchIfMissing = true)
public class ComputeEngineDemo implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ComputeEngineDemo.class);

    @Autowired
    private ClusterManager clusterManager;

    @Override
    public void run(String... args) throws Exception {
        logger.info("========================================");
        logger.info("Distributed Compute Engine Demo");
        logger.info("========================================");

        // Create and register worker nodes (4 for dashboard visualization)
        logger.info("\n1. Creating worker nodes...");
        WorkerNode worker1 = new WorkerNode("worker-1", 4);
        WorkerNode worker2 = new WorkerNode("worker-2", 4);
        WorkerNode worker3 = new WorkerNode("worker-3", 4);
        WorkerNode worker4 = new WorkerNode("worker-4", 4);

        clusterManager.registerWorker(worker1);
        clusterManager.registerWorker(worker2);
        clusterManager.registerWorker(worker3);
        clusterManager.registerWorker(worker4);

        logger.info("Registered {} worker nodes", clusterManager.getAllWorkers().size());
        logger.info("Total available slots: {}", clusterManager.getTotalAvailableSlots());

        // Create a sample job
        logger.info("\n2. Creating a sample job...");
        Job job = createSampleJob();
        logger.info("Job created: {}", job.getDescription());
        logger.info("  - Stages: {}", job.getStageCount());
        logger.info("  - Total tasks: {}", job.getStages().stream()
                .mapToInt(Stage::getTaskCount)
                .sum());

        // Submit job
        logger.info("\n3. Submitting job for execution...");
        clusterManager.submitJob(job);
        logger.info("Job submitted successfully");

        // Monitor job progress
        logger.info("\n4. Monitoring job execution...");
        monitorJobProgress(job, 30); // Monitor for up to 30 seconds

        // Display final statistics
        logger.info("\n5. Final Statistics:");
        displayStatistics();

        logger.info("\n========================================");
        logger.info("Demo completed!");
        logger.info("========================================");
    }

    /**
     * Creates a sample job with multiple stages and tasks.
     */
    private Job createSampleJob() {
        Job job = new Job("Sample Data Processing Job");

        // Stage 1: Data Loading (4 parallel tasks)
        Stage stage1 = new Stage("Stage 1: Data Loading");
        stage1.addTask(new Task(1000, "Load data from source A"));
        stage1.addTask(new Task(1200, "Load data from source B"));
        stage1.addTask(new Task(800, "Load data from source C"));
        stage1.addTask(new Task(1500, "Load data from source D"));
        job.addStage(stage1);

        // Stage 2: Data Transformation (3 parallel tasks)
        Stage stage2 = new Stage("Stage 2: Data Transformation");
        stage2.addTask(new Task(2000, "Transform data set 1"));
        stage2.addTask(new Task(1800, "Transform data set 2"));
        stage2.addTask(new Task(2200, "Transform data set 3"));
        job.addStage(stage2);

        // Stage 3: Data Aggregation (2 parallel tasks)
        Stage stage3 = new Stage("Stage 3: Data Aggregation");
        stage3.addTask(new Task(1500, "Aggregate metrics"));
        stage3.addTask(new Task(1600, "Calculate statistics"));
        job.addStage(stage3);

        // Stage 4: Final Output (1 task)
        Stage stage4 = new Stage("Stage 4: Write Results");
        stage4.addTask(new Task(1000, "Write results to output"));
        job.addStage(stage4);

        return job;
    }

    /**
     * Monitors job progress until completion or timeout.
     */
    private void monitorJobProgress(Job job, int timeoutSeconds) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long timeoutMs = timeoutSeconds * 1000L;
        int lastCompletedCount = -1;
        int lastStageIndex = -1;

        while (!job.isCompleted() && (System.currentTimeMillis() - startTime) < timeoutMs) {
            Thread.sleep(500);

            int currentStageIndex = job.getCurrentStageIndex();
            if (currentStageIndex >= 0) {
                Stage currentStage = job.getCurrentStage();
                if (currentStage != null) {
                    int completed = currentStage.getCompletedTaskCount();
                    int total = currentStage.getTaskCount();
                    
                    // Only log if there's a change to avoid spam
                    if (currentStageIndex != lastStageIndex || completed != lastCompletedCount) {
                        logger.info("  Stage {}: {}/{} tasks completed", 
                                currentStageIndex + 1, completed, total);
                        lastCompletedCount = completed;
                        lastStageIndex = currentStageIndex;
                    }
                }
            }
            
            // Check if job completed after sleep
            if (job.isCompleted()) {
                break;
            }
        }

        if (job.isCompleted()) {
            logger.info("  Job completed successfully!");
        } else {
            logger.warn("  Job monitoring timed out after {} seconds", timeoutSeconds);
        }
    }

    /**
     * Displays cluster and job statistics.
     */
    private void displayStatistics() {
        Map<String, Object> stats = clusterManager.getClusterStats();
        logger.info("  Worker nodes: {}", stats.get("workerCount"));
        logger.info("  Total slots: {}", stats.get("totalSlots"));
        logger.info("  Available slots: {}", stats.get("availableSlots"));
        logger.info("  Running tasks: {}", stats.get("runningTasks"));
        logger.info("  Pending tasks: {}", stats.get("pendingTasks"));
        logger.info("  Total jobs: {}", stats.get("totalJobs"));
        logger.info("  Total tasks scheduled: {}", stats.get("totalTasksScheduled"));

        // Display worker statistics
        logger.info("\n  Worker Statistics:");
        for (WorkerNode worker : clusterManager.getAllWorkers()) {
            logger.info("    {}: {} tasks executed, avg time: {:.2f}ms",
                    worker.getHostname(),
                    worker.getTotalTasksExecuted(),
                    worker.getAverageExecutionTimeMs());
        }
    }
}
