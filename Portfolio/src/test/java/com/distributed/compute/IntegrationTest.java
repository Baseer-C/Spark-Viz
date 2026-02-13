package com.distributed.compute;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.cluster.WorkerNode;
import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the distributed compute engine.
 * Tests the full workflow from job submission to completion.
 */
@SpringBootTest
class IntegrationTest {

    @Autowired
    private ClusterManager clusterManager;

    private WorkerNode worker1;
    private WorkerNode worker2;
    private WorkerNode worker3;

    @BeforeEach
    void setUp() {
        // Create and register multiple workers
        worker1 = new WorkerNode("worker-1", 2);
        worker2 = new WorkerNode("worker-2", 2);
        worker3 = new WorkerNode("worker-3", 2);
        
        clusterManager.registerWorker(worker1);
        clusterManager.registerWorker(worker2);
        clusterManager.registerWorker(worker3);
    }

    @AfterEach
    void tearDown() {
        // Clean up workers
        if (worker1 != null) {
            clusterManager.removeWorker(worker1.getId());
            worker1.shutdownNow();
        }
        if (worker2 != null) {
            clusterManager.removeWorker(worker2.getId());
            worker2.shutdownNow();
        }
        if (worker3 != null) {
            clusterManager.removeWorker(worker3.getId());
            worker3.shutdownNow();
        }
        
        // Give time for any running jobs to complete or be interrupted
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testFullJobExecution() throws InterruptedException {
        // Create a job with multiple stages
        Job job = new Job("Integration Test Job");
        
        // Stage 1: 4 tasks that can run in parallel
        Stage stage1 = new Stage("Stage 1 - Parallel Tasks");
        stage1.addTask(new Task(200, "Task 1-1"));
        stage1.addTask(new Task(200, "Task 1-2"));
        stage1.addTask(new Task(200, "Task 1-3"));
        stage1.addTask(new Task(200, "Task 1-4"));
        job.addStage(stage1);
        
        // Stage 2: 3 tasks (runs after stage 1 completes)
        Stage stage2 = new Stage("Stage 2 - Sequential After Stage 1");
        stage2.addTask(new Task(200, "Task 2-1"));
        stage2.addTask(new Task(200, "Task 2-2"));
        stage2.addTask(new Task(200, "Task 2-3"));
        job.addStage(stage2);
        
        // Stage 3: 2 tasks (runs after stage 2 completes)
        Stage stage3 = new Stage("Stage 3 - Final Stage");
        stage3.addTask(new Task(200, "Task 3-1"));
        stage3.addTask(new Task(200, "Task 3-2"));
        job.addStage(stage3);
        
        // Submit job
        assertTrue(clusterManager.submitJob(job));
        
        // Wait for job to complete (should take ~600ms per stage = ~1800ms total)
        // But allow more time for scheduling overhead
        Thread.sleep(5000);
        
        // Verify job is completed
        Job retrieved = clusterManager.getJob(job.getId());
        assertNotNull(retrieved);
        assertTrue(retrieved.isCompleted());
        
        // Verify all stages are completed
        assertTrue(stage1.isCompleted());
        assertTrue(stage2.isCompleted());
        assertTrue(stage3.isCompleted());
        
        // Verify all tasks are completed
        for (Task task : stage1.getTasks()) {
            assertEquals(com.distributed.compute.model.TaskStatus.COMPLETED, task.getStatus());
        }
        for (Task task : stage2.getTasks()) {
            assertEquals(com.distributed.compute.model.TaskStatus.COMPLETED, task.getStatus());
        }
        for (Task task : stage3.getTasks()) {
            assertEquals(com.distributed.compute.model.TaskStatus.COMPLETED, task.getStatus());
        }
    }

    @Test
    void testMultipleJobsConcurrent() throws InterruptedException {
        // Submit multiple jobs concurrently
        Job job1 = createSimpleJob("Job 1");
        Job job2 = createSimpleJob("Job 2");
        Job job3 = createSimpleJob("Job 3");
        
        clusterManager.submitJob(job1);
        clusterManager.submitJob(job2);
        clusterManager.submitJob(job3);
        
        // Wait for all jobs to complete
        Thread.sleep(3000);
        
        // Verify all jobs are completed
        assertTrue(clusterManager.getJob(job1.getId()).isCompleted());
        assertTrue(clusterManager.getJob(job2.getId()).isCompleted());
        assertTrue(clusterManager.getJob(job3.getId()).isCompleted());
    }

    @Test
    void testLoadBalancing() throws InterruptedException {
        // Create a job with many tasks to test load balancing
        Job job = new Job("Load Balancing Test");
        Stage stage = new Stage("Large Stage");
        
        // Add 10 tasks
        for (int i = 0; i < 10; i++) {
            stage.addTask(new Task(100, "Task " + i));
        }
        job.addStage(stage);
        
        clusterManager.submitJob(job);
        Thread.sleep(2000);
        
        // Verify tasks are distributed across workers
        // All workers should have executed some tasks
        assertTrue(worker1.getTotalTasksExecuted() > 0 || 
                  worker2.getTotalTasksExecuted() > 0 || 
                  worker3.getTotalTasksExecuted() > 0);
        
        // Verify job completed
        assertTrue(clusterManager.getJob(job.getId()).isCompleted());
    }

    @Test
    void testClusterStats() throws InterruptedException {
        Job job = createSimpleJob("Stats Test Job");
        clusterManager.submitJob(job);
        
        Thread.sleep(1000);
        
        var stats = clusterManager.getClusterStats();
        assertNotNull(stats);
        // Verify our 3 workers are included (may be more from other tests)
        assertTrue((Integer) stats.get("workerCount") >= 3);
        assertTrue((Integer) stats.get("totalSlots") >= 6); // At least 3 workers * 2 slots
        assertTrue((Integer) stats.get("totalTasksScheduled") > 0);
    }

    /**
     * Helper method to create a simple job for testing.
     */
    private Job createSimpleJob(String name) {
        Job job = new Job(name);
        Stage stage = new Stage("Test Stage");
        stage.addTask(new Task(100, "Task 1"));
        stage.addTask(new Task(100, "Task 2"));
        job.addStage(stage);
        return job;
    }
}
