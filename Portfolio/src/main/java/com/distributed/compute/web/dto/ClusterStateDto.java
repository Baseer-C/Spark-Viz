package com.distributed.compute.web.dto;

import java.util.List;

/**
 * DTO for full cluster state broadcast over WebSocket.
 */
public record ClusterStateDto(
    long timestamp,
    int pendingTaskCount,
    int totalJobs,
    int totalTasksScheduled,
    int totalTasksCompleted,
    int totalSlots,
    int availableSlots,
    int runningTasks,
    List<WorkerNodeStateDto> workers,
    JobDagDto dag
) {}
