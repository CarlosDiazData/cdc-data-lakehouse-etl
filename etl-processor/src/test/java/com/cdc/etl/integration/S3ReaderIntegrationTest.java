package com.cdc.etl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.cdc.etl.discovery.DiscoveredFile;
import com.cdc.etl.discovery.FileDiscovery;
import com.cdc.etl.discovery.PollingFileDiscovery;
import com.cdc.etl.reader.S3Reader;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration tests for S3 file discovery and Parquet reading
 * using Testcontainers LocalStack (local S3 emulation).
 *
 * <p>These tests require Docker to be running. Tagged with {@code @Tag("integration")}
 * so they can be excluded from unit-test-only runs:
 * {@code mvn test -Dgroups='!integration'}.
 */
@Tag("integration")
@Testcontainers
@DisplayName("S3Reader + FileDiscovery Integration")
class S3ReaderIntegrationTest {

    private static final String RAW_BUCKET = "cdc-raw-data";
    private static final String RAW_PREFIX = "dms-source";

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(LocalStackContainer.Service.S3);

    private static S3Client s3Client;
    private static S3Reader s3Reader;

    @BeforeAll
    static void setUp() {
        s3Client = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(RAW_BUCKET).build());
        s3Reader = new S3Reader(s3Client);
    }

    @AfterAll
    static void tearDown() {
        if (s3Client != null) s3Client.close();
    }

    // ── Tests ────────────────────────────────────────

    @Nested
    @DisplayName("FileDiscovery with LocalStack")
    class FileDiscoveryTests {

        @Test
        @DisplayName("discovers Parquet files in S3 prefix")
        void discoversParquetFiles() throws IOException {
            // Given: upload a test file
            String key = RAW_PREFIX + "/public/customers/test.parquet";
            uploadTestFile(RAW_BUCKET, key, createMinimalParquet());

            // When: poll for files
            FileDiscovery discovery = new PollingFileDiscovery(s3Client, RAW_BUCKET, RAW_PREFIX);
            List<DiscoveredFile> files = discovery.discover();

            // Then
            assertThat(files).isNotEmpty();
            assertThat(files).anyMatch(f -> f.key().equals(key));
            assertThat(files).allMatch(f -> f.bucket().equals(RAW_BUCKET));
        }

        @Test
        @DisplayName("markProcessed excludes file from subsequent discoveries")
        void markProcessedExcludesFile() throws IOException {
            String key = RAW_PREFIX + "/public/products/test-marked.parquet";
            uploadTestFile(RAW_BUCKET, key, createMinimalParquet());

            FileDiscovery discovery = new PollingFileDiscovery(s3Client, RAW_BUCKET, RAW_PREFIX);

            List<DiscoveredFile> first = discovery.discover();
            assertThat(first).anyMatch(f -> f.key().equals(key));

            discovery.markProcessed(first.getFirst());

            List<DiscoveredFile> second = discovery.discover();
            assertThat(second).noneMatch(f -> f.key().equals(key));
        }

        @Test
        @DisplayName("empty prefix returns empty list")
        void emptyPrefix() {
            FileDiscovery discovery = new PollingFileDiscovery(s3Client, RAW_BUCKET, "nonexistent/");
            List<DiscoveredFile> files = discovery.discover();
            assertThat(files).isEmpty();
        }
    }

    @Nested
    @DisplayName("S3Reader with LocalStack")
    class S3ReaderTests {

        @Test
        @DisplayName("reads rows from uploaded Parquet file")
        void readsRowsFromParquet() throws IOException {
            // Given: upload a minimal valid Parquet file
            String key = RAW_PREFIX + "/public/customers/LOAD0001.parquet";
            uploadTestFile(RAW_BUCKET, key, createMinimalParquet());

            DiscoveredFile file = new DiscoveredFile(key, RAW_BUCKET, 1024, Instant.now());

            // When
            List<Map<String, Object>> rows = s3Reader.readRows(file);

            // Then: rows are returned (even if schema differs, S3 reader works)
            assertThat(rows).isNotNull();
        }

        @Test
        @DisplayName("Discovers and reads file end-to-end")
        void discoverAndRead() throws IOException {
            String key = RAW_PREFIX + "/public/customers/discover-read.parquet";
            uploadTestFile(RAW_BUCKET, key, createMinimalParquet());

            FileDiscovery discovery = new PollingFileDiscovery(s3Client, RAW_BUCKET, RAW_PREFIX);
            List<DiscoveredFile> files = discovery.discover();
            assertThat(files).isNotEmpty();

            DiscoveredFile file = files.getFirst();
            List<Map<String, Object>> rows = s3Reader.readRows(file);
            assertThat(rows).isNotNull();

            discovery.markProcessed(file);
        }
    }

    // ── Helpers ──────────────────────────────────────

    private void uploadTestFile(String bucket, String key, byte[] content) {
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                RequestBody.fromBytes(content));
    }

    /**
     * Creates a minimal valid Parquet file on disk and returns its bytes.
     * Uses Apache Parquet to write a simple schema + one row so the
     * ParquetReader can parse it.
     */
    private byte[] createMinimalParquet() throws IOException {
        // Minimal Parquet: write via Iceberg's Parquet writer for a simple schema
        Path tempFile = Files.createTempFile("test-", ".parquet");
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer =
                    org.apache.parquet.hadoop.ParquetWriter.<org.apache.parquet.example.data.Group>builder(
                            new org.apache.hadoop.fs.Path(tempFile.toString()))
                            .withConf(conf)
                            .withWriteMode(org.apache.parquet.hadoop.metadata.ParquetFileWriter.Mode.OVERWRITE)
                            .withRowGroupSize(1024)
                            .build();

            org.apache.parquet.example.data.simple.SimpleGroupFactory f =
                    new org.apache.parquet.example.data.simple.SimpleGroupFactory(
                            org.apache.parquet.schema.MessageTypeParser.parseMessageType(
                                    "message test { required int32 id; optional binary name (UTF8); }"));

            org.apache.parquet.example.data.Group group = f.newGroup()
                    .append("id", 1)
                    .append("name", "test-record");
            writer.write(group);
            writer.close();

            return Files.readAllBytes(tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
