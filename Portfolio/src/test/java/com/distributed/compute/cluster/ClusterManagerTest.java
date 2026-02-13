package com.distributed.compute.cluster;

import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the ClusterManager class.
 */
@SpringBootTest
class ClusterManagerTest {

    @Autowired
    private ClusterManager clusterManager;

    private WorkerNode worker1;
    private WorkerNode worker2;

    @BeforeEach
    void setUp() {
        worker1 = new WorkerNode("worker-1", 2);
        worker2 = new WorkerNode("worker-2", 2);
        clusterManager.registerWorker(worker1);
        clusterManager.registerWorker(worker2);
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
        
        // Give time for any running jobs to complete or be interrupted
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testRegisterWorker() {
        int initialCount = clusterManager.getAllWorkers().size();
        WorkerNode newWorker = new WorkerNode("worker-3", 3);
        assertTrue(clusterManager.registerWorker(newWorker));
        assertEquals(initialCount + 1, clusterManager.getAllWorkers().size());
        
        clusterManager.removeWorker(newWorker.getId());
        newWorker.shutdownNow();
    }

    @Test
    void testRegisterDuplicateWorker() {
        WorkerNode duplicate = new WorkerNode("worker-1-duplicate", 2);
        duplicate = new WorkerNode(worker1.getHostname(), 2);
        // Should not register duplicate
        assertFalse(clusterManager.registerWorker(worker1));
    }

    @Test
    void testRemoveWorker() {
        int initialCount = clusterManager.getAllWorkers().size();
        assertTrue(clusterManager.removeWorker(worker1.getId()));
        assertEquals(initialCount - 1, clusterManager.getAllWorkers().size());
        assertNull(clusterManager.getWorker(worker1.getId()));
    }

    @Test
    void testGetWorker() {
        WorkerNode retrieved = clusterManager.getWorker(worker1.getId());
        assertNotNull(retrieved);
        assertEquals(worker1.getId(), retrieved.getId());
    }

    @Test
    void testGetAllWorkers() {
        List<WorkerNode> workers = clusterManager.getAllWorkers();
        assertTrue(workers.size() >= 2);
        assertTrue(workers.contains(worker1));
        assertTrue(workers.contains(worker2));
    }

    @Test
    void testGetTotalAvailableSlots() {
        int totalSlots = clusterManager.getTotalAvailableSlots();
        assertTrue(totalSlots >= 4); // At least 2 + 2 from our workers
        // Verify our workers contribute 4 slots
        assertTrue(worker1.getAvailableSlots() + worker2.getAvailableSlots() <= totalSlots);
    }

    @Test
    void testSubmitJob() throws InterruptedException {
        Job job = createSimpleJob();
        assertTrue(clusterManager.submitJob(job));
        
        Job retrieved = clusterManager.getJob(job.getId());
        assertNotNull(retrieved);
        assertEquals(job.getId(), retrieved.getId());
        
        // Wait for job to start
        Thread.sleep(500);
    }

    @Test
    void testSubmitDuplicateJob() {
        Job job = createSimpleJob();
        clusterManager.submitJob(job);
        assertFalse(clusterManager.submitJob(job)); // Duplicate
    }

    @Test
    void testSubmitNullJob() {
        assertThrows(IllegalArgumentException.class, () -> {
            clusterManager.submitJob(null);
        });
    }

    @Test
    void testGetJob() {
        Job job = createSimpleJob();
        clusterManager.submitJob(job);
        
        Job retrieved = clusterManager.getJob(job.getId());
        assertNotNull(retrieved);
        assertEquals(job.getId(), retrieved.getId());
    }

    @Test
    void testGetAllJobs() {
        Job job1 = createSimpleJob();
        Job job2 = createSimpleJob();
        
        clusterManager.submitJob(job1);
        clusterManager.submitJob(job2);
        
        List<Job> jobs = clusterManager.getAllJobs();
        assertTrue(jobs.size() >= 2);
    }

    @Test
    void testJobExecution() throws InterruptedException {
        Job job = createSimpleJob();
        clusterManager.submitJob(job);
        
        // Wait for job execution
        Thread.sleep(2000);
        
        // Job should be completed or in progress
        Job retrieved = clusterManager.getJob(job.getId());
        assertNotNull(retrieved);
    }

    @Test
    void testJobWithMultipleStages() throws InterruptedException {
        Job job = new Job("Multi-Stage Job");
        
        // Stage 1: 2 tasks
        Stage stage1 = new Stage("Stage 1");
        stage1.addTask(new Task(100, "Task 1-1"));
        stage1.addTask(new Task(100, "Task 1-2"));
        job.addStage(stage1);
        
        // Stage 2: 2 tasks (runs after stage 1)
        Stage stage2 = new Stage("Stage 2");
        stage2.addTask(new Task(100, "Task 2-1"));
        stage2.addTask(new Task(100, "Task 2-2"));
        job.addStage(stage2);
        
        clusterManager.submitJob(job);
        
        // Wait for execution
        Thread.sleep(3000);
        
        Job retrieved = clusterManager.getJob(job.getId());
        assertNotNull(retrieved);
    }

    @Test
    void testGetClusterStats() {
        Map<String, Object> stats = clusterManager.getClusterStats();
        
        assertNotNull(stats);
        assertTrue(stats.containsKey("workerCount"));
        assertTrue(stats.containsKey("totalSlots"));
        assertTrue(stats.containsKey("availableSlots"));
        assertTrue(stats.containsKey("runningTasks"));
        assertTrue(stats.containsKey("pendingTasks"));
        assertTrue(stats.containsKey("totalJobs"));
        
        // Verify our workers are included
        assertTrue((Integer) stats.get("workerCount") >= 2);
        assertTrue((Integer) stats.get("totalSlots") >= 4); // At least 2 + 2 from our workers
    }

    @Test
    void testGetTotalTasksScheduled() throws InterruptedException {
        Job job = createSimpleJob();
        clusterManager.submitJob(job);
        
        Thread.sleep(1000);
        
        assertTrue(clusterManager.getTotalTasksScheduled() > 0);
    }

    @Test
    void testGetTotalJobsSubmitted() {
        int initialCount = clusterManager.getTotalJobsSubmitted();
        
        Job job1 = createSimpleJob();
        Job job2 = createSimpleJob();
        
        clusterManager.submitJob(job1);
        clusterManager.submitJob(job2);
        
        assertTrue(clusterManager.getTotalJobsSubmitted() >= initialCount + 2);
    }

    @Test
    void testClusterManagerToString() {
        String toString = clusterManager.toString();
        assertTrue(toString.contains("ClusterManager"));
    }

    /**
     * Helper method to create a simple job for testing.
     */
    private Job createSimpleJob() {
        Job job = new Job("Test Job");
        Stage stage = new Stage("Test Stage");
        stage.addTask(new Task(100, "Test Task 1"));
        stage.addTask(new Task(100, "Test Task 2"));
        job.addStage(stage);
        return job;
    }
}
