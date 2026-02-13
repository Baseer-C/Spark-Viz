package com.distributed.compute.web;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.cluster.WorkerNode;
import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import com.distributed.compute.model.TaskStatus;
import com.distributed.compute.web.dto.ClusterStateDto;
import com.distributed.compute.web.dto.JobDagDto;
import com.distributed.compute.web.dto.StageDagDto;
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
        int totalMemoryMb = (Integer) stats.getOrDefault("totalMemoryMb", 0);
        int usedMemoryMb = (Integer) stats.getOrDefault("usedMemoryMb", 0);

        List<WorkerNodeStateDto> workers = new ArrayList<>();
        long totalCompleted = 0;
        for (WorkerNode w : clusterManager.getAllWorkers()) {
            workers.add(buildWorkerState(w));
            totalCompleted += w.getTotalTasksExecuted();
        }

        JobDagDto dag = buildDag();

        return new ClusterStateDto(
                System.currentTimeMillis(),
                pendingCount,
                totalJobs,
                totalScheduled,
                (int) totalCompleted,
                totalSlots,
                availableSlots,
                runningTasks,
                totalMemoryMb,
                usedMemoryMb,
                workers,
                dag
        );
    }

    /**
     * Build DAG summary from the most recently active job (non-completed first, else last).
     */
    private JobDagDto buildDag() {
        List<Job> allJobs = clusterManager.getAllJobs();
        if (allJobs.isEmpty()) {
            return null;
        }
        Job job = allJobs.stream()
                .filter(j -> !j.isCompleted())
                .findFirst()
                .orElse(allJobs.get(allJobs.size() - 1));

        List<Stage> stageList = job.getStages();
        List<StageDagDto> stageDtos = new ArrayList<>();
        for (int i = 0; i < stageList.size(); i++) {
            Stage s = stageList.get(i);
            stageDtos.add(new StageDagDto(
                    i,
                    s.getDescription(),
                    s.getTaskCount(),
                    s.getCompletedTaskCount(),
                    s.isCompleted()
            ));
        }
        return new JobDagDto(job.getId(), job.getDescription(), stageDtos);
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

        int totalMem = w.getTotalMemoryMb();
        if (totalMem == Integer.MAX_VALUE) totalMem = 0;
        return new WorkerNodeStateDto(
                w.getId(),
                w.getHostname(),
                w.getTotalSlots(),
                w.getAvailableSlots(),
                totalMem,
                w.getUsedMemoryMb(),
                w.getRunningTaskCount(),
                w.getTotalTasksExecuted(),
                health,
                taskDtos
        );
    }
}
