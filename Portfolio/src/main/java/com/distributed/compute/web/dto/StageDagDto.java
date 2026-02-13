package com.distributed.compute.web.dto;

/**
 * One stage in the DAG view (for UI).
 */
public record StageDagDto(
    int stageIndex,
    String description,
    int taskCount,
    int completedCount,
    boolean completed
) {}
