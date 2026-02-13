package com.distributed.compute.web.dto;

/**
 * Request body for adding a new worker node.
 */
public record AddWorkerRequest(
    String hostname,
    Integer slots,
    Integer memoryMb
) {}
