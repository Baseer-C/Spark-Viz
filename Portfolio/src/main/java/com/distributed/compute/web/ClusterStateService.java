package com.distributed.compute.web;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.cluster.WorkerNode;
import com.distributed.compute.model.Task;
import com.distributed.compute.model.TaskStatus;
import com.distributed.compute.web.dto.ClusterStateDto;
import com.distributed.compute.web.dto.TaskStateDto;
import com.distributed.compute.web.dto.WorkerNodeStateDto;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds cluster state DTO from ClusterManager for WebSocket broadcast.
 */
@Service
public class ClusterStateService {

    private final ClusterManager clusterManager;

    public ClusterStateService(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public ClusterStateDto buildClusterState() {
        Map<String, Object> stats = clusterManager.getClusterStats();
        int pendingCount = (Integer) stats.getOrDefault("pendingTasks", 0);
        int totalJobs = (Integer) stats.getOrDefault("totalJobs", 0);
        int totalScheduled = (Integer) stats.getOrDefault("totalTasksScheduled", 0);
        int totalSlots = (Integer) stats.getOrDefault("totalSlots", 0);
        int availableSlots = (Integer) stats.getOrDefault("availableSlots", 0);
        int runningTasks = (Integer) stats.getOrDefault("runningTasks", 0);

        List<WorkerNodeStateDto> workers = new ArrayList<>();
        long totalCompleted = 0;
        for (WorkerNode w : clusterManager.getAllWorkers()) {
            workers.add(buildWorkerState(w));
            totalCompleted += w.getTotalTasksExecuted();
        }

        return new ClusterStateDto(
                System.currentTimeMillis(),
                pendingCount,
                totalJobs,
                totalScheduled,
                (int) totalCompleted,
                totalSlots,
                availableSlots,
                runningTasks,
                workers
        );
    }

    private WorkerNodeStateDto buildWorkerState(WorkerNode w) {
        List<Task> running = w.getRunningTasksSnapshot();
        List<TaskStateDto> taskDtos = new ArrayList<>();
        for (Task t : running) {
            taskDtos.add(TaskStateDto.from(t.getId(), t.getStatus(), t.getDescription()));
        }
        int emptySlots = w.getTotalSlots() - running.size();
        for (int i = 0; i < emptySlots; i++) {
            taskDtos.add(TaskStateDto.from("slot-" + w.getId() + "-" + i, TaskStatus.PENDING, ""));
        }

        String health = "ALIVE";
        try {
            if (w.getAvailableSlots() < 0 || w.getTotalSlots() == 0) {
                health = "UNHEALTHY";
            }
        } catch (Exception e) {
            health = "UNKNOWN";
        }

        return new WorkerNodeStateDto(
                w.getId(),
                w.getHostname(),
                w.getTotalSlots(),
                w.getAvailableSlots(),
                w.getRunningTaskCount(),
                w.getTotalTasksExecuted(),
                health,
                taskDtos
        );
    }
}
