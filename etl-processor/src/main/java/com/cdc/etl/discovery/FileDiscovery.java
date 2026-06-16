package com.cdc.etl.discovery;

import java.util.List;

/**
 * Strategy interface for discovering raw CDC Parquet files.
 * <p>
 * Decouples the "how files are discovered" concern from the processing pipeline.
 * Current implementation: {@link PollingFileDiscovery} (S3 listObjectsV2 polling).
 * Future: {@code SqsFileDiscovery} (S3 Event Notifications → SQS).
 * <p>
 * The contract guarantees that discovered files are not yet processed; callers
 * MUST call {@link #markProcessed(DiscoveredFile)} after successful processing
 * to prevent duplicate reads.
 */
public interface FileDiscovery {

    /**
     * Discover unprocessed raw Parquet files in the configured S3 prefix.
     *
     * @return list of discovered files (empty if none pending)
     */
    List<DiscoveredFile> discover();

    /**
     * Mark a file as successfully processed so future discovery calls
     * exclude it. Implementation may use an S3 marker object, an Iceberg
     * table property, or an in-memory manifest.
     *
     * @param file the file that was processed
     */
    void markProcessed(DiscoveredFile file);
}
