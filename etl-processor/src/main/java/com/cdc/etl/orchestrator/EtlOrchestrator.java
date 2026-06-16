package com.cdc.etl.orchestrator;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.cdc.etl.discovery.DiscoveredFile;
import com.cdc.etl.discovery.FileDiscovery;
import com.cdc.etl.glue.GlueCatalogSync;
import com.cdc.etl.iceberg.IcebergWriter;
import com.cdc.etl.reader.S3Reader;
import com.cdc.etl.transformer.Transformer;
import com.cdc.etl.transformer.TransformationResult;

/**
 * Scheduled orchestrator for the CDC ETL pipeline.
 * <p>
 * Runs on a configurable interval (default 60s, driven by {@code etl.discovery.polling-interval-seconds})
 * and executes the full Bronze → Gold pipeline:
 * <ol>
 *   <li>{@link FileDiscovery#discover()} — find new Parquet files in S3 raw zone</li>
 *   <li>{@link S3Reader#readRows(DiscoveredFile)} — stream-deserialize Parquet</li>
 *   <li>{@link Transformer#transform(Map)} — dedup, Star Schema assembly, SCD-2 handling</li>
 *   <li>{@link IcebergWriter#writeAll(TransformationResult)} — commit to Iceberg tables</li>
 *   <li>{@link GlueCatalogSync#syncTable(String, String, Map)} — register metadata in Glue</li>
 * </ol>
 * <p>
 * Error resilience:
 * <ul>
 *   <li>Malformed Parquet: logged + skipped (file preserved for debugging)</li>
 *   <li>Transient S3/Glue failure: @Retryable with exponential backoff</li>
 *   <li>Crash mid-batch: Iceberg snapshot atomicity ensures no partial writes</li>
 * </ul>
 */
@Component
public class EtlOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(EtlOrchestrator.class);

    private final FileDiscovery fileDiscovery;
    private final S3Reader s3Reader;
    private final Transformer transformer;
    private final IcebergWriter icebergWriter;
    private final GlueCatalogSync glueSync;
    private final String warehousePath;

    private int errorCounter = 0;

    /** Per-file error counters keyed by S3 URI (for tracking malformed rows per file). */
    private final Map<String, Integer> perFileErrors = new ConcurrentHashMap<>();

    /** Total errors across all batches. */
    private final AtomicInteger totalErrors = new AtomicInteger(0);

    public EtlOrchestrator(
            FileDiscovery fileDiscovery,
            S3Reader s3Reader,
            Transformer transformer,
            IcebergWriter icebergWriter,
            GlueCatalogSync glueSync,
            @Value("${iceberg.warehouse}") String warehousePath) {
        this.fileDiscovery = fileDiscovery;
        this.s3Reader = s3Reader;
        this.transformer = transformer;
        this.icebergWriter = icebergWriter;
        this.glueSync = glueSync;
        this.warehousePath = warehousePath;
    }

    /**
     * Main ETL loop — triggered by Spring's scheduler at the configured interval.
     * <p>
     * Processes all discovered files in one batch per tick. If no files are
     * pending, this is a no-op (minimal overhead).
     */
    @Scheduled(fixedDelayString = "${etl.discovery.polling-interval-seconds:60}000")
    public void processBatch() {
        log.debug("ETL polling tick started");

        try {
            glueSync.ensureDatabase();

            List<DiscoveredFile> files = fileDiscovery.discover();
            if (files.isEmpty()) {
                log.debug("No new files to process.");
                return;
            }

            log.info("Processing {} discovered Parquet file(s)", files.size());

            // Group records by source table using file path convention:
            // dms-source/public/{table}/... → table name is the parent directory
            Map<String, List<Map<String, Object>>> recordsByTable = new LinkedHashMap<>();
            int batchErrors = 0;

            for (DiscoveredFile file : files) {
                try {
                    List<Map<String, Object>> rows = s3Reader.readRows(file);
                    String table = extractTableName(file.key());
                    recordsByTable.computeIfAbsent(table, k -> new java.util.ArrayList<>()).addAll(rows);
                } catch (Exception e) {
                    log.error("Error processing file {} (row-level context unavailable) — skipping. Reason: {}",
                            file.uri(), e.getMessage(), e);
                    errorCounter++;
                    batchErrors++;
                    perFileErrors.merge(file.uri(), 1, Integer::sum);
                }
            }

            if (recordsByTable.isEmpty()) {
                log.warn("No valid records extracted from {} discovered file(s)", files.size());
                return;
            }

            // Transform → Star Schema
            TransformationResult result = transformer.transform(recordsByTable);

            // Write → Iceberg tables
            icebergWriter.writeAll(result);

            // Sync → Glue Catalog
            String lakehouseBucket = warehousePath.replace("s3://", "").split("/")[0];
            syncAllTables(lakehouseBucket);

            // Mark files as processed
            for (DiscoveredFile file : files) {
                fileDiscovery.markProcessed(file);
            }

            log.info("ETL batch complete: {} file(s) processed, {} dimensional rows, {} fact rows",
                    files.size(),
                    result.customers().size() + result.products().size() + result.dates().size(),
                    result.facts().size());

            if (batchErrors > 0) {
                log.warn("Batch error summary: {} file-level error(s) in this batch, {} total errors across all batches",
                        batchErrors, totalErrors.addAndGet(batchErrors));
                perFileErrors.forEach((uri, count) ->
                        log.warn("  File: {} — {} error(s)", uri, count));
            }

        } catch (Exception e) {
            log.error("ETL batch failed: {}", e.getMessage(), e);
            errorCounter++;
            totalErrors.incrementAndGet();
        }
    }

    /**
     * Return the total error count across all batches.
     */
    public int getErrorCount() {
        return totalErrors.get();
    }

    /**
     * Return per-file error counts (immutable snapshot).
     */
    public Map<String, Integer> getPerFileErrors() {
        return Map.copyOf(perFileErrors);
    }

    /**
     * Return a human-readable error summary for the last batch.
     */
    public String getErrorSummary() {
        int total = totalErrors.get();
        if (total == 0) {
            return "No errors recorded.";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Total errors: %d%n", total));
        sb.append("Per-file breakdown:\n");
        perFileErrors.forEach((uri, count) ->
                sb.append(String.format("  %s — %d error(s)%n", uri, count)));
        return sb.toString();
    }

    // ── Private helpers ─────────────────────────────

    /**
     * Extract the source table name from a DMS Parquet S3 key.
     * <p>
     * Expected convention: {@code dms-source/public/{table}/LOAD*.parquet}
     *
     * @param key S3 object key
     * @return table name (e.g. "customers", "orders")
     */
    private String extractTableName(String key) {
        // dms-source/public/customers/LOAD00000001.parquet → "customers"
        String[] parts = key.split("/");
        if (parts.length >= 3) {
            return parts[2];  // Third segment is the table name
        }
        // Fallback: use the parent directory of the file
        for (int i = parts.length - 2; i >= 0; i--) {
            if (!parts[i].isEmpty()) {
                return parts[i];
            }
        }
        return "unknown";
    }

    private void syncAllTables(String lakehouseBucket) {
        Map<String, String> properties = Map.of(
                "table_type", "ICEBERG",
                "format-version", "2");

        glueSync.syncTable("dim_customer", "s3://" + lakehouseBucket + "/iceberg/dim_customer", properties);
        glueSync.syncTable("dim_product", "s3://" + lakehouseBucket + "/iceberg/dim_product", properties);
        glueSync.syncTable("dim_date", "s3://" + lakehouseBucket + "/iceberg/dim_date", properties);
        glueSync.syncTable("fact_orders", "s3://" + lakehouseBucket + "/iceberg/fact_orders", properties);

        Map<String, Boolean> verification = glueSync.verifyTables();
        log.info("Glue table registration: {}", verification);
    }
}
