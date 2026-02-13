package com.distributed.compute.web.dto;

import java.util.List;

/**
 * DAG summary of a job (stages in order) for the UI.
 */
public record JobDagDto(
    String jobId,
    String jobDescription,
    List<StageDagDto> stages
) {}
