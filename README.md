# Distributed Compute Engine Simulation

A simplified Apache Spark-like distributed compute engine simulation built with Java and Spring Boot.

## Architecture Overview

This project simulates a DAG (Directed Acyclic Graph) execution engine with the following core components:

### Core Domain Models

1. **Task** (`com.distributed.compute.model.Task`)
   - Represents a unit of work
   - Has unique ID, status (PENDING, RUNNING, COMPLETED, FAILED), and duration
   - Uses `AtomicReference` for thread-safe status management
   - Simulates work via `Thread.sleep()`

2. **Stage** (`com.distributed.compute.model.Stage`)
   - Groups independent Tasks that can run in parallel
   - Uses `ConcurrentHashMap` for O(1) task lookup
   - Uses `AtomicInteger` for completion tracking

3. **Job** (`com.distributed.compute.model.Job`)
   - Contains Stages that must execute sequentially
   - Stage N+1 cannot start until Stage N completes
   - Uses `ConcurrentHashMap` for stage management
   - Uses `AtomicReference` for current stage tracking

4. **WorkerNode** (`com.distributed.compute.cluster.WorkerNode`)
   - Simulates a server with a fixed number of execution slots
   - Uses `ThreadPoolExecutor` for concurrent task execution
   - Uses `ConcurrentHashMap` to track running tasks
   - Uses `AtomicInteger` for slot availability tracking

5. **ClusterManager** (`com.distributed.compute.cluster.ClusterManager`)
   - Central coordinator ("brain") of the cluster
   - Manages all WorkerNodes and assigns Tasks to available slots
   - Uses `ConcurrentHashMap` for worker and job state management
   - Implements round-robin scheduling for load balancing

## Thread Safety Design

All components are designed for concurrent access:

- **ConcurrentHashMap**: Used throughout for O(1) lookups with fine-grained locking
- **AtomicReference**: Used for single-value state updates (e.g., Task status, current stage)
- **AtomicInteger**: Used for counters (completion counts, slot availability)
- **Synchronized Collections**: Used for thread-safe iteration where needed

## Key Design Decisions

1. **ConcurrentHashMap over synchronized HashMap**: Provides better concurrent performance with fine-grained locking
2. **AtomicReference for status**: Enables lock-free status transitions using compare-and-set semantics
3. **ThreadPoolExecutor**: Handles thread lifecycle and provides built-in queue management
4. **Round-robin scheduling**: Ensures fair distribution of tasks across workers

## Building and Running

### Build the Project

```bash
# Build the project (compiles and runs tests)
mvn clean install

# Build without running tests
mvn clean install -DskipTests
```

### Run the Application

```bash
# Run the Spring Boot application (includes demo)
mvn spring-boot:run
```

When you run the application, it will automatically execute a demo that:
- Creates 3 worker nodes with 4 slots each
- Submits a sample job with 4 stages
- Monitors job execution progress
- Displays final statistics

### Run Unit Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TaskTest

# Run tests with verbose output
mvn test -X
```

### Test Coverage

The project includes comprehensive unit tests:

- **TaskTest**: Tests Task lifecycle, status transitions, and thread safety
- **StageTest**: Tests Stage management, task grouping, and completion tracking
- **JobTest**: Tests Job execution, sequential stage processing, and state management
- **WorkerNodeTest**: Tests worker node task execution, slot management, and statistics
- **ClusterManagerTest**: Tests cluster coordination, job submission, and scheduling
- **IntegrationTest**: Tests end-to-end job execution across multiple workers

All tests use JUnit 5 and are located in `src/test/java/`.

## Project Structure

```
src/main/java/com/distributed/compute/
├── DistributedComputeApplication.java  # Spring Boot entry point
├── model/
│   ├── Task.java                       # Task domain model
│   ├── TaskStatus.java                 # Task status enumeration
│   ├── Stage.java                      # Stage domain model
│   └── Job.java                        # Job domain model
├── cluster/
│   ├── WorkerNode.java                 # Worker node implementation
│   └── ClusterManager.java             # Cluster coordinator
└── demo/
    └── ComputeEngineDemo.java          # Demo/example implementation

src/test/java/com/distributed/compute/
├── model/
│   ├── TaskTest.java                   # Task unit tests
│   ├── StageTest.java                  # Stage unit tests
│   └── JobTest.java                    # Job unit tests
└── cluster/
    ├── WorkerNodeTest.java             # WorkerNode unit tests
    ├── ClusterManagerTest.java         # ClusterManager unit tests
    └── IntegrationTest.java           # End-to-end integration tests
```

## Usage Example

```java
// Create worker nodes
WorkerNode worker1 = new WorkerNode("worker-1", 4); // 4 slots
WorkerNode worker2 = new WorkerNode("worker-2", 4);

// Register workers with cluster manager
clusterManager.registerWorker(worker1);
clusterManager.registerWorker(worker2);

// Create a job with stages
Job job = new Job("Example Job");

// Create stage 1 with parallel tasks
Stage stage1 = new Stage("Stage 1");
stage1.addTask(new Task(1000, "Task 1-1"));
stage1.addTask(new Task(1500, "Task 1-2"));
stage1.addTask(new Task(2000, "Task 1-3"));
job.addStage(stage1);

// Create stage 2 (runs after stage 1 completes)
Stage stage2 = new Stage("Stage 2");
stage2.addTask(new Task(1000, "Task 2-1"));
stage2.addTask(new Task(1000, "Task 2-2"));
job.addStage(stage2);

// Submit job for execution
clusterManager.submitJob(job);
```

## Requirements

- Java 17+
- Maven 3.6+
- Spring Boot 3.2.0
