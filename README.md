# ⚡ Distributed Compute Engine (Mini-Spark)

> A resilient, distributed DAG (Directed Acyclic Graph) execution engine built with Java, Spring Boot, and WebSockets. Designed to simulate fault tolerance, concurrent task scheduling, and real-time cluster monitoring.

---

## 🎥 Live Demo (Short Preview)
Watch the **Fault Tolerance** in action: notice how tasks automatically reschedule when a node is killed.

<video src="(https://github.com/Baseer-C/Spark-Viz/issues/2#issue-3938418243)" controls="controls" muted="muted" autoplay="autoplay" loop="loop" style="max-width: 100%;"></video>

> **[📺 Click here to watch the Full Narrated Walkthrough (Deep Dive)]( (https://github.com/Baseer-C/Spark-Viz/issues/1#issue-3938401092) )**
> *See the full DAG visualization, straggler simulation, and worker lifecycle management.*

---

## 🚀 Key Features

### 1. Visualized Fault Tolerance ("The Chaos Monkey")
* **Feature:** Instantly kill any worker node to simulate hardware failure.
* **Outcome:** The `ClusterManager` detects the heartbeat failure, marks the node as `DEAD`, and strictly re-queues all incomplete tasks to healthy nodes without data loss.

### 2. DAG Task Execution
* **Logic:** Jobs are broken into **Stages** (sequential) and **Tasks** (parallel).
* **Dependency:** Stage `N+1` cannot begin until all tasks in Stage `N` are completed (Barriers).
* **Visualization:** Real-time DAG rendering at the bottom of the dashboard shows stage progression and bottlenecks.

### 3. Resource-Aware Scheduling
* **Constraint:** Tasks are not just assigned to open threads; they are scheduled based on simulated **CPU & Memory constraints**.
* **Algorithm:** Implements a custom bin-packing strategy to ensure efficient cluster utilization.

---

## 🛠️ Tech Stack

* **Core:** Java 17, Spring Boot
* **Concurrency:** `ConcurrentHashMap`, `AtomicInteger`, `ThreadPoolExecutor`
* **Real-Time:** WebSockets (STOMP), Spring Messaging
* **Frontend:** HTML5, CSS Grid (Dark Mode), JavaScript (No frameworks, pure DOM manipulation)
* **Testing:** JUnit 5, Mockito (80+ Unit Tests)

---

## 🏗 System Architecture

The system follows a classic **Master-Worker** architecture:

1.  **ClusterManager (The "Brain"):**
    * Holds the global state of the cluster.
    * manages the `JobQueue` and `TaskScheduler`.
    * Broadcasts state updates to the UI via WebSockets every 500ms.
2.  **WorkerNodes (The "Muscle"):**
    * Independent threads simulating distributed servers.
    * Each node has a fixed slot capacity (CPU cores).
    * Executes tasks with simulated duration (using `Thread.sleep`).
3.  **The Communication Layer:**
    * Nodes report status updates (COMPLETED, FAILED) back to the Manager.
    * Manager pushes "Heartbeat" checks to ensure nodes are alive.

---

## 🚀 How to Run

### Prerequisites
* Java 17+
* Maven 3.6+

### Steps
1.  **Build the project:**
    ```bash
    mvn clean install
    ```
2.  **Run the application:**
    ```bash
    mvn spring-boot:run
    ```
3.  **Open the Dashboard:**
    * Go to: `http://localhost:8080`
    * Click **"Run Job"** to start the simulation.
    * Click **"Kill Node"** on any worker to test resilience.

---

## 🧪 Testing

The project includes a full suite of unit and integration tests covering the scheduling logic and concurrency safety.

```bash
# Run all tests
mvn test

# Run specific concurrency test
mvn test -Dtest=ClusterManagerTest
