package com.cdc.etl.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import com.cdc.etl.discovery.DiscoveredFile;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Reads CDC Parquet files from the S3 raw zone with streaming deserialization.
 * <p>
 * Downloads each file from S3 to a temporary local path, then uses Apache Parquet's
 * {@link ParquetReader} to stream rows one-at-a-time without loading the entire
 * dataset into memory. Supports Snappy compression (handled automatically by the
 * Parquet library).
 * <p>
 * Returns rows as {@code List<Map<String, Object>>} where each map represents a
 * single row with column-name → value pairs. CDC metadata columns (Op, Timestamp,
 * etc.) from DMS are included.
 */
@Component
public class S3Reader {

    private static final Logger log = LoggerFactory.getLogger(S3Reader.class);

    private final S3Client s3Client;

    public S3Reader(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Read all rows from a discovered Parquet file.
     * <p>
     * Retries on transient S3 failures with exponential backoff
     * (3 attempts, 2s/4s/8s).
     *
     * @param file the discovered file to read
     * @return list of row maps; each map is column-name → value
     */
    @Retryable(
            retryFor = { IOException.class, RuntimeException.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, maxDelay = 8000, multiplier = 2.0))
    public List<Map<String, Object>> readRows(DiscoveredFile file) throws IOException {
        log.info("Reading Parquet file: {}", file.uri());

        Path tempFile = null;
        try {
            // Download to temp file
            tempFile = Files.createTempFile("cdc-parquet-", ".parquet");
            downloadToTemp(file, tempFile);

            // Parse schema first
            ParquetMetadata metadata = ParquetFileReader.readFooter(
                    new Configuration(), new org.apache.hadoop.fs.Path(tempFile.toString()));
            List<String> fieldNames = metadata.getFileMetaData().getSchema().getFields().stream()
                    .map(f -> f.getName())
                    .toList();

            // Stream-read rows with per-row error handling
            List<Map<String, Object>> rows = new ArrayList<>();
            int malformedRows = 0;
            try (ParquetReader<Group> reader = ParquetReader.builder(
                    new GroupReadSupport(), new org.apache.hadoop.fs.Path(tempFile.toString())).build()) {

                Group group;
                int rowNum = 0;
                while ((group = reader.read()) != null) {
                    rowNum++;
                    try {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 0; i < fieldNames.size(); i++) {
                            row.put(fieldNames.get(i), extractValue(group, i));
                        }
                        rows.add(row);
                    } catch (Exception e) {
                        log.warn("Malformed row #{} in {} — skipping. Reason: {}",
                                rowNum, file.uri(), e.getMessage());
                        malformedRows++;
                    }
                }
            }

            if (malformedRows > 0) {
                log.warn("{} malformed row(s) skipped in {} ({} valid rows retained)",
                        malformedRows, file.uri(), rows.size());
            }
            log.info("Read {} rows from {}", rows.size(), file.uri());
            return rows;

        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {
                    log.warn("Failed to delete temp file {}: {}", tempFile, e.getMessage());
                }
            }
        }
    }

    /**
     * Read rows as a lazy stream to minimize memory pressure for large files.
     * <p>
     * Note: the stream is backed by a temp file; the caller MUST close the stream
     * to release resources. Consider using try-with-resources.
     *
     * @param file the discovered file to read
     * @return a closeable stream of row maps
     */
    public Stream<Map<String, Object>> readRowsStreaming(DiscoveredFile file) throws IOException {
        log.info("Streaming Parquet file: {}", file.uri());

        Path tempFile = Files.createTempFile("cdc-parquet-stream-", ".parquet");
        downloadToTemp(file, tempFile);

        ParquetMetadata metadata = ParquetFileReader.readFooter(
                new Configuration(), new org.apache.hadoop.fs.Path(tempFile.toString()));
        List<String> fieldNames = metadata.getFileMetaData().getSchema().getFields().stream()
                .map(f -> f.getName())
                .toList();

        ParquetReader<Group> reader = ParquetReader.builder(
                new GroupReadSupport(), new org.apache.hadoop.fs.Path(tempFile.toString())).build();

        // Lazy stream with per-row error handling for malformed records
        final int[] rowNum = {0};
        final int[] malformed = {0};
        var iterable = (Iterable<Group>) () -> new ParquetReaderIterator(reader);
        Stream<Map<String, Object>> stream = StreamSupport.stream(iterable.spliterator(), false)
                .map(group -> {
                    rowNum[0]++;
                    try {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 0; i < fieldNames.size(); i++) {
                            row.put(fieldNames.get(i), extractValue(group, i));
                        }
                        return row;
                    } catch (Exception e) {
                        log.warn("Malformed row #{} in {} — skipping. Reason: {}",
                                rowNum[0], file.uri(), e.getMessage());
                        malformed[0]++;
                        return null;
                    }
                })
                .filter(java.util.Objects::nonNull);

        // Clean up temp file when stream is closed
        return stream.onClose(() -> {
            try {
                reader.close();
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                log.warn("Cleanup failed for {}: {}", tempFile, e.getMessage());
            }
        });
    }

    // ── Private helpers ─────────────────────────────

    private void downloadToTemp(DiscoveredFile file, Path tempFile) throws IOException {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(file.bucket())
                .key(file.key())
                .build();

        try (ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(request)) {
            Files.copy(s3Stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        log.debug("Downloaded {} ({} bytes) to {}", file.uri(), Files.size(tempFile), tempFile);
    }

    /**
     * Extract a typed value from a Parquet Group at the given field index.
     * Handles common types: INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN, BINARY.
     */
    private Object extractValue(Group group, int fieldIndex) {
        int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
        if (repetitionCount == 0) {
            return null;
        }

        // Parquet SimpleGroup stores values by type; try each
        try {
            return group.getInteger(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getLong(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getFloat(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getDouble(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getString(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getBoolean(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        try {
            return group.getBinary(fieldIndex, 0);
        } catch (Exception ignored) { /* try next type */ }

        log.warn("Unhandled Parquet type at field index {}", fieldIndex);
        return group.getValueToString(fieldIndex, 0);
    }

    // ── Iterator wrapper ────────────────────────────

    private static class ParquetReaderIterator implements java.util.Iterator<Group> {
        private final ParquetReader<Group> reader;
        private Group next;
        private boolean done;

        ParquetReaderIterator(ParquetReader<Group> reader) {
            this.reader = reader;
            advance();
        }

        private void advance() {
            try {
                next = reader.read();
                if (next == null) {
                    done = true;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Parquet record", e);
            }
        }

        @Override
        public boolean hasNext() {
            return !done;
        }

        @Override
        public Group next() {
            Group current = next;
            advance();
            return current;
        }
    }
}
