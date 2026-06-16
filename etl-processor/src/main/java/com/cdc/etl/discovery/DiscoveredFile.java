package com.cdc.etl.discovery;

import java.time.Instant;

/**
 * Immutable representation of a discovered CDC Parquet file in the S3 raw zone.
 *
 * @param key        S3 object key (e.g. {@code dms-source/public/orders/LOAD00000001.parquet})
 * @param bucket     S3 bucket name
 * @param size       file size in bytes
 * @param lastModified last modification timestamp (from S3 metadata)
 */
public record DiscoveredFile(
        String key,
        String bucket,
        long size,
        Instant lastModified
) {
    /**
     * Full S3 URI for this file.
     */
    public String uri() {
        return "s3://" + bucket + "/" + key;
    }

    /**
     * Human-readable size string (e.g. "1.2 MB").
     */
    public String sizeFormatted() {
        if (size < 1024) return size + " B";
        double kb = size / 1024.0;
        if (kb < 1024) return String.format("%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format("%.1f MB", mb);
        return String.format("%.1f GB", mb / 1024.0);
    }
}
