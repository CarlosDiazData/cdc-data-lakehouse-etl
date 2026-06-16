package com.cdc.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * CDC Data Lakehouse — ETL Processor main entry point.
 * <p>
 * This Spring Boot application reads CDC Parquet files from the S3 raw zone (BRONZE),
 * transforms them into a Star Schema, writes Apache Iceberg tables to the lakehouse zone
 * (GOLD), and registers metadata in the Glue Data Catalog.
 * <p>
 * Scheduled polling via {@link EnableScheduling} drives the file discovery loop;
 * {@link EnableRetry} provides exponential backoff for transient S3/Glue failures.
 */
@SpringBootApplication
@EnableScheduling
@EnableRetry
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
