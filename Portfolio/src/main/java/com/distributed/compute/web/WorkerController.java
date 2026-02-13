package com.distributed.compute.web;

import com.distributed.compute.cluster.ClusterManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for worker actions (e.g. kill node).
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
