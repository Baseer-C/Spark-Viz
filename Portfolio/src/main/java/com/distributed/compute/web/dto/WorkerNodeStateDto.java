package com.distributed.compute.web.dto;

import java.util.List;

/**
 * DTO for worker node state in cluster-state WebSocket payload.
 */
public record WorkerNodeStateDto(
    String id,
    String hostname,
    int totalSlots,
    int availableSlots,
    int totalMemoryMb,
    int usedMemoryMb,
    int runningTaskCount,
    long totalTasksExecuted,
    String health,
    List<TaskStateDto> tasks
) {}
