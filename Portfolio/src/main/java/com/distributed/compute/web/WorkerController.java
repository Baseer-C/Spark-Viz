package com.distributed.compute.web;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.cluster.WorkerNode;
import com.distributed.compute.web.dto.AddWorkerRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for worker actions (add node, kill node).
 */
@RestController
@RequestMapping("/api/workers")
@CrossOrigin(origins = "*")
public class WorkerController {

    private final ClusterManager clusterManager;

    public WorkerController(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Add a new worker node to the cluster.
     */
    @PostMapping
    public ResponseEntity<?> addWorker(@RequestBody AddWorkerRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", "Request body required"));
        }
        int slots = request.slots() != null && request.slots() > 0 ? request.slots() : 4;
        int memoryMb = request.memoryMb() != null && request.memoryMb() >= 0 ? request.memoryMb() : 4096;
        String hostname = request.hostname() != null && !request.hostname().isBlank()
            ? request.hostname()
            : "worker-" + System.currentTimeMillis();
        WorkerNode worker = new WorkerNode(hostname, slots, memoryMb);
        boolean registered = clusterManager.registerWorker(worker);
        if (!registered) {
            return ResponseEntity.unprocessableEntity()
                .body(Map.of("success", false, "message", "Failed to register worker"));
        }
        return ResponseEntity.ok(Map.of(
            "success", true,
            "workerId", worker.getId(),
            "hostname", worker.getHostname(),
            "slots", worker.getTotalSlots(),
            "memoryMb", worker.getTotalMemoryMb() == Integer.MAX_VALUE ? 0 : worker.getTotalMemoryMb()
        ));
    }

    /**
     * Kill (remove) a worker node from the cluster.
     */
    @PostMapping("/{workerId}/kill")
    public ResponseEntity<?> killWorker(@PathVariable String workerId) {
        boolean removed = clusterManager.removeWorker(workerId);
        if (removed) {
            return ResponseEntity.ok(Map.of("success", true, "message", "Worker " + workerId + " removed"));
        }
        return ResponseEntity.notFound().build();
    }
}
