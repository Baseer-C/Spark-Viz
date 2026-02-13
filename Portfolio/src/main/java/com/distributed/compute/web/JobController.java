package com.distributed.compute.web;

import com.distributed.compute.cluster.ClusterManager;
import com.distributed.compute.model.Job;
import com.distributed.compute.model.Stage;
import com.distributed.compute.model.Task;
import com.distributed.compute.web.dto.SubmitJobRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Random;

/**
 * REST controller for submitting jobs from the UI.
 */
@RestController
@RequestMapping("/api/jobs")
@CrossOrigin(origins = "*")
public class JobController {

    private final ClusterManager clusterManager;

    public JobController(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Submit a new job. Request body defines stages (task count and duration per stage).
     */
    @PostMapping("/submit")
    public ResponseEntity<?> submitJob(@RequestBody SubmitJobRequest request) {
        if (request == null || request.stages() == null || request.stages().isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of("success", false, "message", "At least one stage with taskCount > 0 required"));
        }

        String description = request.description() != null && !request.description().isBlank()
            ? request.description()
            : "Job-" + System.currentTimeMillis();

        boolean simulateDataSkew = Boolean.TRUE.equals(request.simulateDataSkew());
        int stragglerStageIndex = 1; // Stage 2 (0-based)
        int stragglerTaskDurationMs = 10_000;

        Job job = new Job(description);
        Random rng = new Random();
        for (int i = 0; i < request.stages().size(); i++) {
            SubmitJobRequest.StageSpec spec = request.stages().get(i);
            int taskCount = Math.max(1, spec.taskCount());
            int durationMs = spec.taskDurationMs() > 0 ? spec.taskDurationMs() : 1000;
            Stage stage = new Stage("Stage " + (i + 1));

            int stragglerTaskIndex = -1;
            if (simulateDataSkew && i == stragglerStageIndex && request.stages().size() > stragglerStageIndex) {
                stragglerTaskIndex = rng.nextInt(taskCount);
            }

            int taskMemoryMb = request.taskMemoryMb() != null && request.taskMemoryMb() >= 0 ? request.taskMemoryMb() : 0;
            for (int t = 0; t < taskCount; t++) {
                int taskDuration = (t == stragglerTaskIndex) ? stragglerTaskDurationMs : durationMs;
                String taskDesc = (t == stragglerTaskIndex) ? "Task " + (t + 1) + " (straggler)" : "Task " + (t + 1);
                stage.addTask(new Task(taskDuration, taskDesc, taskMemoryMb));
            }
            job.addStage(stage);
        }

        boolean submitted = clusterManager.submitJob(job);
        if (!submitted) {
            return ResponseEntity.unprocessableEntity()
                .body(Map.of("success", false, "message", "Job already submitted or submission failed"));
        }

        return ResponseEntity.ok(Map.of(
            "success", true,
            "jobId", job.getId(),
            "description", job.getDescription(),
            "stageCount", job.getStageCount()
        ));
    }
}
