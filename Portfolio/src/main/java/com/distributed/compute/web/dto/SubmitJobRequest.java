package com.distributed.compute.web.dto;

import java.util.List;

/**
 * Request body for submitting a new job from the UI.
 */
public record SubmitJobRequest(
    String description,
    List<StageSpec> stages,
    Boolean simulateDataSkew
) {
    public record StageSpec(int taskCount, int taskDurationMs) {}
}
