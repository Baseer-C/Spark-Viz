package com.distributed.compute.cluster;

import com.distributed.compute.model.Task;
import com.distributed.compute.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the WorkerNode class.
 */
class WorkerNodeTest {

    private WorkerNode workerNode;

    @BeforeEach
    void setUp() {
        workerNode = new WorkerNode("test-worker", 2); // 2 slots
    }

    @AfterEach
    void tearDown() {
        workerNode.shutdownNow();
    }

    @Test
    void testWorkerNodeCreation() {
        assertNotNull(workerNode.getId());
        assertEquals("test-worker", workerNode.getHostname());
        assertEquals(2, workerNode.getTotalSlots());
        assertEquals(2, workerNode.getAvailableSlots());
        assertTrue(workerNode.hasAvailableSlots());
        assertEquals(0, workerNode.getRunningTaskCount());
    }

    @Test
    void testWorkerNodeCreationWithInvalidSlots() {
        assertThrows(IllegalArgumentException.class, () -> {
            new WorkerNode("invalid", 0);
        });
    }

    @Test
    void testSubmitTask() throws Exception {
        Task task = new Task(100, "Test Task");
        Future<Void> future = workerNode.submitTask(task);

        assertNotNull(future);
        assertEquals(TaskStatus.RUNNING, task.getStatus());
        assertEquals(1, workerNode.getAvailableSlots());
        assertEquals(1, workerNode.getRunningTaskCount());

        // Wait for task to complete
        future.get(2, TimeUnit.SECONDS);
        assertEquals(TaskStatus.COMPLETED, task.getStatus());
        assertEquals(2, workerNode.getAvailableSlots());
        assertEquals(0, workerNode.getRunningTaskCount());
    }

    @Test
    void testSubmitMultipleTasks() throws Exception {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");

        Future<Void> future1 = workerNode.submitTask(task1);
        Future<Void> future2 = workerNode.submitTask(task2);

        assertNotNull(future1);
        assertNotNull(future2);
        assertEquals(0, workerNode.getAvailableSlots());
        assertEquals(2, workerNode.getRunningTaskCount());

        // Wait for tasks to complete
        future1.get(2, TimeUnit.SECONDS);
        future2.get(2, TimeUnit.SECONDS);

        assertEquals(2, workerNode.getAvailableSlots());
        assertEquals(0, workerNode.getRunningTaskCount());
    }

    @Test
    void testSubmitTaskWhenAtCapacity() throws Exception {
        Task task1 = new Task(1000, "Task 1");
        Task task2 = new Task(1000, "Task 2");
        Task task3 = new Task(100, "Task 3");

        // Fill all slots
        workerNode.submitTask(task1);
        workerNode.submitTask(task2);

        // Try to submit third task - should be rejected
        assertThrows(RejectedExecutionException.class, () -> {
            workerNode.submitTask(task3);
        });
    }

    @Test
    void testSubmitNullTask() {
        assertThrows(IllegalArgumentException.class, () -> {
            workerNode.submitTask(null);
        });
    }

    @Test
    void testSubmitTaskThatCannotStart() throws Exception {
        Task task = new Task(100, "Test Task");
        task.start();
        task.complete(); // Task already completed

        // Task cannot be started again
        assertThrows(RejectedExecutionException.class, () -> {
            workerNode.submitTask(task);
        });
    }

    @Test
    void testGetRunningTask() throws Exception {
        Task task = new Task(500, "Test Task");
        Future<Void> future = workerNode.submitTask(task);

        Task runningTask = workerNode.getRunningTask(task.getId());
        assertNotNull(runningTask);
        assertEquals(task.getId(), runningTask.getId());

        future.get(1, TimeUnit.SECONDS);
        assertNull(workerNode.getRunningTask(task.getId()));
    }

    @Test
    void testTaskExecutionStatistics() throws Exception {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(200, "Task 2");

        assertEquals(0, workerNode.getTotalTasksExecuted());
        assertEquals(0, workerNode.getTotalExecutionTimeMs());
        assertEquals(0.0, workerNode.getAverageExecutionTimeMs());

        Future<Void> future1 = workerNode.submitTask(task1);
        Future<Void> future2 = workerNode.submitTask(task2);

        future1.get(1, TimeUnit.SECONDS);
        future2.get(1, TimeUnit.SECONDS);

        assertEquals(2, workerNode.getTotalTasksExecuted());
        assertTrue(workerNode.getTotalExecutionTimeMs() > 0);
        assertTrue(workerNode.getAverageExecutionTimeMs() > 0);
    }

    @Test
    void testShutdown() throws Exception {
        Task task = new Task(500, "Test Task");
        Future<Void> future = workerNode.submitTask(task);

        // Shutdown should wait for running tasks
        boolean shutdown = workerNode.shutdown(2000);
        assertTrue(shutdown);
    }

    @Test
    void testShutdownNow() throws Exception {
        Task task = new Task(5000, "Long Task");
        workerNode.submitTask(task);

        workerNode.shutdownNow();
        // Worker should be shut down immediately
    }

    @Test
    void testWorkerNodeToString() {
        String toString = workerNode.toString();
        assertTrue(toString.contains("WorkerNode"));
        assertTrue(toString.contains("test-worker"));
        assertTrue(toString.contains("2")); // total slots
    }
}
