# Testing Guide

This document provides detailed information about running tests for the Distributed Compute Engine.

## Quick Start

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TaskTest

# Run tests with coverage (requires jacoco plugin)
mvn clean test jacoco:report
```

## Test Structure

### Unit Tests

All unit tests are located in `src/test/java/com/distributed/compute/`:

1. **TaskTest** (`model/TaskTest.java`)
   - Tests Task creation, status transitions, and execution
   - Verifies thread-safe status updates
   - Tests task equality and toString methods

2. **StageTest** (`model/StageTest.java`)
   - Tests Stage creation and task management
   - Verifies completion tracking
   - Tests concurrent task operations

3. **JobTest** (`model/JobTest.java`)
   - Tests Job creation and stage management
   - Verifies sequential stage execution
   - Tests stage advancement logic

4. **WorkerNodeTest** (`cluster/WorkerNodeTest.java`)
   - Tests WorkerNode creation and configuration
   - Verifies task submission and execution
   - Tests slot management and capacity limits
   - Tests statistics tracking

5. **ClusterManagerTest** (`cluster/ClusterManagerTest.java`)
   - Tests worker registration and management
   - Verifies job submission and execution
   - Tests task scheduling and load balancing
   - Tests cluster statistics

### Integration Tests

6. **IntegrationTest** (`IntegrationTest.java`)
   - Tests end-to-end job execution
   - Verifies multi-stage job completion
   - Tests concurrent job execution
   - Tests load balancing across workers

## Running Tests in IDE

### IntelliJ IDEA
1. Right-click on `src/test/java` folder
2. Select "Run All Tests"
3. Or right-click on individual test class and select "Run"

### Eclipse
1. Right-click on project
2. Select "Run As" → "JUnit Test"
3. Or right-click on test class and select "Run As" → "JUnit Test"

### VS Code
1. Install Java Test Runner extension
2. Click on test icons in the editor
3. Or use Command Palette: "Java: Run Tests"

## Test Execution Examples

### Run All Tests
```bash
mvn test
```

### Run Specific Test Class
```bash
mvn test -Dtest=TaskTest
mvn test -Dtest=ClusterManagerTest
```

### Run Multiple Test Classes
```bash
mvn test -Dtest=TaskTest,StageTest,JobTest
```

### Run Tests Matching Pattern
```bash
mvn test -Dtest=*Test
mvn test -Dtest=*NodeTest
```

### Run with Verbose Output
```bash
mvn test -X
```

### Skip Tests During Build
```bash
mvn clean install -DskipTests
```

## Expected Test Results

When all tests pass, you should see output like:

```
[INFO] Tests run: 50+, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

## Troubleshooting

### Tests Fail Due to Timing Issues
Some tests involve timing (e.g., task execution). If tests fail intermittently:
- Increase timeout values in test methods
- Check system load (tests may run slower under load)

### Port Already in Use
If you see port conflicts:
- Change `server.port` in `application.properties`
- Or stop other Spring Boot applications

### Out of Memory
If tests fail with OutOfMemoryError:
```bash
mvn test -DargLine="-Xmx512m"
```

## Continuous Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: mvn test
```

## Test Coverage

To generate test coverage reports (requires jacoco plugin):

```bash
mvn clean test jacoco:report
```

Coverage report will be generated in `target/site/jacoco/index.html`
