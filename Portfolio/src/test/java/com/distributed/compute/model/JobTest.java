package com.distributed.compute.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Job class.
 */
class JobTest {

    private Job job;

    @BeforeEach
    void setUp() {
        job = new Job("Test Job");
    }

    @Test
    void testJobCreation() {
        assertNotNull(job.getId());
        assertEquals("Test Job", job.getDescription());
        assertEquals(0, job.getStageCount());
        assertEquals(-1, job.getCurrentStageIndex());
        assertFalse(job.isCompleted());
    }

    @Test
    void testJobCreationWithDefaultDescription() {
        Job jobWithDefault = new Job();
        assertNotNull(jobWithDefault.getId());
        assertNotNull(jobWithDefault.getDescription());
        assertTrue(jobWithDefault.getDescription().contains("Job-"));
    }

    @Test
    void testAddStage() {
        Stage stage = new Stage("Stage 1");
        assertTrue(job.addStage(stage));
        assertEquals(1, job.getStageCount());
        assertEquals(stage, job.getStage(stage.getId()));
    }

    @Test
    void testAddNullStage() {
        assertFalse(job.addStage(null));
        assertEquals(0, job.getStageCount());
    }

    @Test
    void testAddMultipleStages() {
        List<Stage> stages = new ArrayList<>();
        stages.add(new Stage("Stage 1"));
        stages.add(new Stage("Stage 2"));
        stages.add(new Stage("Stage 3"));

        job.addStages(stages);
        assertEquals(3, job.getStageCount());
    }

    @Test
    void testGetStageById() {
        Stage stage = new Stage("Stage 1");
        job.addStage(stage);

        Stage retrieved = job.getStage(stage.getId());
        assertNotNull(retrieved);
        assertEquals(stage.getId(), retrieved.getId());
    }

    @Test
    void testGetStageByIndex() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        job.addStage(stage1);
        job.addStage(stage2);

        assertEquals(stage1, job.getStage(0));
        assertEquals(stage2, job.getStage(1));
        assertNull(job.getStage(2)); // Out of bounds
        assertNull(job.getStage(-1)); // Invalid index
    }

    @Test
    void testGetStages() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        job.addStage(stage1);
        job.addStage(stage2);

        List<Stage> stages = job.getStages();
        assertEquals(2, stages.size());
        assertEquals(stage1, stages.get(0));
        assertEquals(stage2, stages.get(1));
    }

    @Test
    void testStartFirstStage() {
        Stage stage1 = new Stage("Stage 1");
        job.addStage(stage1);

        assertTrue(job.startFirstStage());
        assertEquals(0, job.getCurrentStageIndex());
        assertEquals(stage1, job.getCurrentStage());
    }

    @Test
    void testStartFirstStageWithNoStages() {
        assertFalse(job.startFirstStage());
        assertEquals(-1, job.getCurrentStageIndex());
    }

    @Test
    void testStartFirstStageWhenAlreadyStarted() {
        Stage stage1 = new Stage("Stage 1");
        job.addStage(stage1);
        job.startFirstStage();

        assertFalse(job.startFirstStage()); // Cannot start again
        assertEquals(0, job.getCurrentStageIndex());
    }

    @Test
    void testAdvanceToNextStage() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        Task task1 = new Task(100, "Task 1");
        stage1.addTask(task1);
        job.addStage(stage1);
        job.addStage(stage2);

        job.startFirstStage();
        assertEquals(0, job.getCurrentStageIndex());

        // Complete stage 1
        task1.start();
        task1.complete();
        stage1.incrementCompletedCount();

        assertTrue(job.advanceToNextStage());
        assertEquals(1, job.getCurrentStageIndex());
        assertEquals(stage2, job.getCurrentStage());
        assertEquals(1, job.getCompletedStageCount());
    }

    @Test
    void testAdvanceToNextStageWhenCurrentNotCompleted() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        job.addStage(stage1);
        job.addStage(stage2);

        job.startFirstStage();
        assertFalse(job.advanceToNextStage()); // Stage 1 not completed
        assertEquals(0, job.getCurrentStageIndex());
    }

    @Test
    void testAdvanceToNextStageAtLastStage() {
        Stage stage1 = new Stage("Stage 1");
        Task task1 = new Task(100, "Task 1");
        stage1.addTask(task1);
        job.addStage(stage1);

        job.startFirstStage();
        task1.start();
        task1.complete();
        stage1.incrementCompletedCount();

        assertFalse(job.advanceToNextStage()); // Already at last stage
        assertEquals(0, job.getCurrentStageIndex());
    }

    @Test
    void testCompleteCurrentStage() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        Task task1 = new Task(100, "Task 1");
        stage1.addTask(task1);
        job.addStage(stage1);
        job.addStage(stage2);

        job.startFirstStage();
        task1.start();
        task1.complete();
        stage1.incrementCompletedCount();

        assertTrue(job.completeCurrentStage());
        assertEquals(1, job.getCurrentStageIndex());
    }

    @Test
    void testIsCompleted() {
        Stage stage1 = new Stage("Stage 1");
        Task task1 = new Task(100, "Task 1");
        stage1.addTask(task1);
        job.addStage(stage1);

        assertFalse(job.isCompleted());

        job.startFirstStage();
        assertFalse(job.isCompleted());

        task1.start();
        task1.complete();
        stage1.incrementCompletedCount();
        assertTrue(job.isCompleted());
    }

    @Test
    void testIsCompletedWithMultipleStages() {
        Stage stage1 = new Stage("Stage 1");
        Stage stage2 = new Stage("Stage 2");
        Task task1 = new Task(100, "Task 1");
        Task task2 = new Task(100, "Task 2");
        stage1.addTask(task1);
        stage2.addTask(task2);
        job.addStage(stage1);
        job.addStage(stage2);

        assertFalse(job.isCompleted());

        job.startFirstStage();
        task1.start();
        task1.complete();
        stage1.incrementCompletedCount();
        job.advanceToNextStage();

        assertFalse(job.isCompleted()); // Stage 2 not completed

        task2.start();
        task2.complete();
        stage2.incrementCompletedCount();
        assertTrue(job.isCompleted());
    }

    @Test
    void testJobEquality() {
        Job job1 = new Job("Job 1");
        Job job2 = new Job("Job 1");

        assertNotEquals(job1, job2); // Different IDs
        assertEquals(job1, job1); // Same instance
    }

    @Test
    void testJobToString() {
        String toString = job.toString();
        assertTrue(toString.contains("Job"));
        assertTrue(toString.contains(job.getId()));
    }
}
