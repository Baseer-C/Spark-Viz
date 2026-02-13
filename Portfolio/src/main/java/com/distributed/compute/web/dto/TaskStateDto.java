package com.distributed.compute.web.dto;

import com.distributed.compute.model.TaskStatus;

/**
 * DTO for task state in cluster-state WebSocket payload.
 */
public record TaskStateDto(String id, String status, String description) {
    public static TaskStateDto from(String id, TaskStatus status, String description) {
        return new TaskStateDto(id, status.name(), description != null ? description : "");
    }
}
