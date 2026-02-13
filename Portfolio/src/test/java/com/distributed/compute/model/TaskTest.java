package com.distributed.compute.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Task class.
 */
class TaskTest {

    private Task task;

    @BeforeEach
    void setUp() {
        task = new Task(1000, "Test Task");
    }

    @Test
    void testTaskCreation() {
        assertNotNull(task.getId());
        assertEquals(TaskStatus.PENDING, task.getStatus());
        assertEquals(1000, task.getDurationMs());
        assertEquals("Test Task", task.getDescription());
    }

    @Test
    void testTaskCreationWithDefaultDescription() {
        Task taskWithDefault = new Task(500);
        assertNotNull(taskWithDefault.getId());
        assertNotNull(taskWithDefault.getDescription());
        assertTrue(taskWithDefault.getDescription().contains("Task-"));
    }

    @Test
    void testStartTask() {
        assertTrue(task.start());
        assertEquals(TaskStatus.RUNNING, task.getStatus());
    }

    @Test
    void testStartTaskWhenNotPending() {
        task.start();
        assertFalse(task.start()); // Cannot start again
        assertEquals(TaskStatus.RUNNING, task.getStatus());
    }

    @Test
    void testCompleteTask() {
        task.start();
        assertTrue(task.complete());
        assertEquals(TaskStatus.COMPLETED, task.getStatus());
    }

    @Test
    void testCompleteTaskWhenNotRunning() {
        assertFalse(task.complete()); // Cannot complete if not running
        assertEquals(TaskStatus.PENDING, task.getStatus());
    }

    @Test
    void testFailTask() {
        task.start();
        assertTrue(task.fail());
        assertEquals(TaskStatus.FAILED, task.getStatus());
    }

    @Test
    void testFailTaskWhenNotRunning() {
        assertFalse(task.fail()); // Cannot fail if not running
        assertEquals(TaskStatus.PENDING, task.getStatus());
    }

    @Test
    void testUpdateStatusWithCompareAndSet() {
        assertTrue(task.updateStatus(TaskStatus.PENDING, TaskStatus.RUNNING));
        assertEquals(TaskStatus.RUNNING, task.getStatus());
    }

    @Test
    void testUpdateStatusWithWrongExpectedStatus() {
        assertFalse(task.updateStatus(TaskStatus.RUNNING, TaskStatus.COMPLETED));
        assertEquals(TaskStatus.PENDING, task.getStatus());
    }

    @Test
    void testExecuteTask() throws InterruptedException {
        task.start();
        long startTime = System.currentTimeMillis();
        task.execute();
        long endTime = System.currentTimeMillis();
        
        assertTrue(endTime - startTime >= 1000 - 50); // Allow 50ms tolerance
    }

    @Test
    void testTaskEquality() {
        Task task1 = new Task(1000, "Task 1");
        Task task2 = new Task(1000, "Task 1");
        
        assertNotEquals(task1, task2); // Different IDs
        assertEquals(task1, task1); // Same instance
    }

    @Test
    void testTaskHashCode() {
        Task task1 = new Task(1000, "Task 1");
        Task task2 = new Task(1000, "Task 1");
        
        assertNotEquals(task1.hashCode(), task2.hashCode()); // Different IDs
        assertEquals(task1.hashCode(), task1.hashCode()); // Same instance
    }

    @Test
    void testTaskToString() {
        String toString = task.toString();
        assertTrue(toString.contains("Task"));
        assertTrue(toString.contains(task.getId()));
        assertTrue(toString.contains("PENDING"));
    }
}
