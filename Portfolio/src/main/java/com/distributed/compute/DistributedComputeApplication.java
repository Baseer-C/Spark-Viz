package com.distributed.compute;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main Spring Boot application entry point for the distributed compute engine simulation.
 * This application simulates a simplified Apache Spark-like execution engine.
 */
@SpringBootApplication
@EnableScheduling
public class DistributedComputeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedComputeApplication.class, args);
    }
}
