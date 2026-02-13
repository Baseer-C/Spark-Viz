package com.distributed.compute.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Stage class.
 */
class StageTest {

    private Stage stage;

    @BeforeEach
    void setUp() {
        stage = new Stage("Test Stage");
    }

    @Test
    void testStageCreation() {
        assertNotNull(stage.getId());
        assertEquals("Test Stage", stage.getDescription());
        assertEquals(0, stage.getTaskCount());
        assertFalse(stage.isCompleted());
    }

    @Test
    void testStageCreationWithDefaultDescription() {
        Stage stageWithDefault = new Stage();
        assertNotNull(stageWithDefault.getId());
        assertNotNull(stageWithDefault.getDescription());
        assertTrue(stageWithDefault.getDescription().contains("Stage-"));
    }

    @Test
    void testAddTask() {
        Task task = new Task(1000, "Task 1");
        assertTrue(stage.addTask(task));
        assertEquals(1, stage.getTaskCount());
        assertEquals(task, stage.getTask(task.getId()));
    }

    @Test
    void testAddNullTask() {
        assertFalse(stage.addTask(null));
        assertEquals(0, stage.getTaskCount());
    }

    @Test
    void testAddMultipleTasks() {
        List<Task> tasks = new ArrayList<>();
        tasks.add(new Task(1000, "Task 1"));
        tasks.add(new Task(2000, "Task 2"));
        tasks.add(new Task(3000, "Task 3"));

        stage.addTasks(tasks);
        assertEquals(3, stage.getTaskCount());
    }

    @Test
    void testGetTask() {
        Task task = new Task(1000, "Task 1");
        stage.addTask(task);
        
        Task retrieved = stage.getTask(task.getId());
        assertNotNull(retrieved);
        assertEquals(task.getId(), retrieved.getId());
    }

    @Test
    void testGetTaskNotFound() {
        assertNull(stage.getTask("non-existent-id"));
    }

    @Test
    void testGetTasks() {
        Task task1 = new Task(1000, "Task 1");
        Task task2 = new Task(2000, "Task 2");
        stage.addTask(task1);
        stage.addTask(task2);

        List<Task> tasks = stage.getTasks();
        assertEquals(2, tasks.size());
        assertTrue(tasks.contains(task1));
        assertTrue(tasks.contains(task2));
    }

    @Test
    void testIsCompletedWithNoTasks() {
        assertFalse(stage.isCompleted()); // Empty stage is not completed
    }

    @Test
    void testIsCompletedWithAllTasksCompleted() {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");
        stage.addTask(task1);
        stage.addTask(task2);

        assertFalse(stage.isCompleted()); // Tasks not completed yet

        // Simulate task completion
        task1.start();
        task1.complete();
        stage.incrementCompletedCount();

        task2.start();
        task2.complete();
        stage.incrementCompletedCount();

        assertTrue(stage.isCompleted());
    }

    @Test
    void testIncrementCompletedCount() {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");
        stage.addTask(task1);
        stage.addTask(task2);

        assertEquals(0, stage.getCompletedTaskCount());
        stage.incrementCompletedCount();
        assertEquals(1, stage.getCompletedTaskCount());
        stage.incrementCompletedCount();
        assertEquals(2, stage.getCompletedTaskCount());
    }

    @Test
    void testGetPendingTaskCount() {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");
        stage.addTask(task1);
        stage.addTask(task2);

        assertEquals(2, stage.getPendingTaskCount());

        task1.start();
        assertEquals(1, stage.getPendingTaskCount());
    }

    @Test
    void testGetRunningTaskCount() {
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");
        stage.addTask(task1);
        stage.addTask(task2);

        assertEquals(0, stage.getRunningTaskCount());

        task1.start();
        assertEquals(1, stage.getRunningTaskCount());

        task2.start();
        assertEquals(2, stage.getRunningTaskCount());
    }

    @Test
    void testStageEquality() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 1");

        assertNotEquals(stage1, stage2); // Different IDs
        assertEquals(stage1, stage1); // Same instance
    }

    @Test
    void testStageToString() {
        String toString = stage.toString();
        assertTrue(toString.contains("Stage"));
        assertTrue(toString.contains(stage.getId()));
    }
}
