package com.cdc.etl.discovery;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Polling-based file discovery using S3 {@code listObjectsV2}.
 * <p>
 * Scheduled via Spring {@code @Scheduled} at a configurable interval (default 60s).
 * Tracks processed files via an in-memory set — this is suitable for single-instance
 * daemon mode. For multi-instance deployments, swap to an S3 marker-object manifest
 * or Iceberg table property.
 * <p>
 * Designed per the Strategy pattern: this class implements {@link FileDiscovery}
 * and can be swapped for {@code SqsFileDiscovery} with zero changes to the pipeline.
 */
@Component
public class PollingFileDiscovery implements FileDiscovery {

    private static final Logger log = LoggerFactory.getLogger(PollingFileDiscovery.class);

    private final S3Client s3Client;
    private final String rawBucket;
    private final String rawPrefix;
    private final Set<String> processedKeys = ConcurrentHashMap.newKeySet();

    public PollingFileDiscovery(
            S3Client s3Client,
            @Value("${aws.s3.raw-bucket}") String rawBucket,
            @Value("${etl.raw-prefix}") String rawPrefix) {
        this.s3Client = s3Client;
        this.rawBucket = rawBucket;
        this.rawPrefix = rawPrefix;
    }

    /**
     * List unprocessed Parquet files in the raw S3 prefix.
     * <p>
     * Retries on transient S3 failures (5xx) with exponential backoff:
     * 2s initial, 4s max, multiplier 2.0, max 3 attempts.
     *
     * @return list of discovered files, sorted by last-modified ascending (oldest first)
     */
    @Override
    @Retryable(
            retryFor = { RuntimeException.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, maxDelay = 4000, multiplier = 2.0))
    public List<DiscoveredFile> discover() {
        log.debug("Polling S3 bucket '{}' with prefix '{}'", rawBucket, rawPrefix);

        List<DiscoveredFile> files = new ArrayList<>();
        String continuationToken = null;

        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(rawBucket)
                    .prefix(rawPrefix);

            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

            for (S3Object s3Object : response.contents()) {
                String key = s3Object.key();

                // Only consider Parquet files that haven't been processed yet
                if (key.endsWith(".parquet") && !processedKeys.contains(key)) {
                    files.add(new DiscoveredFile(
                            key,
                            rawBucket,
                            s3Object.size(),
                            s3Object.lastModified()));
                }
            }

            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);

        // Sort by last-modified ascending: process oldest files first
        Collections.sort(files, (a, b) -> a.lastModified().compareTo(b.lastModified()));

        if (!files.isEmpty()) {
            log.info("Discovered {} unprocessed Parquet files in s3://{}/{}",
                    files.size(), rawBucket, rawPrefix);
        } else {
            log.debug("No new Parquet files found.");
        }

        return files;
    }

    /**
     * Mark a file as processed so it won't be re-discovered.
     * <p>
     * Currently uses an in-memory set. For multi-instance deployments,
     * override this method to persist markers in S3 or Iceberg metadata.
     *
     * @param file the file that was successfully processed
     */
    @Override
    public void markProcessed(DiscoveredFile file) {
        processedKeys.add(file.key());
        log.debug("Marked file as processed: {}", file.key());
    }
}
