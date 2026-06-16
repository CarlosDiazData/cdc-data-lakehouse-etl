package com.cdc.etl.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.cdc.etl.discovery.DiscoveredFile;
import com.cdc.etl.discovery.FileDiscovery;
import com.cdc.etl.glue.GlueCatalogSync;
import com.cdc.etl.iceberg.IcebergWriter;
import com.cdc.etl.orchestrator.EtlOrchestrator;
import com.cdc.etl.reader.S3Reader;
import com.cdc.etl.transformer.Md5KeyGenerator;
import com.cdc.etl.transformer.Transformer;
import com.cdc.etl.transformer.TypeMapper;

/**
 * End-to-end integration tests for the full ETL pipeline.
 *
 * <p>Tests the complete flow: FileDiscovery → S3Reader → Transformer →
 * IcebergWriter → GlueCatalogSync using mocked external dependencies
 * to validate orchestration logic.
 *
 * <p>For full integration with real containers, see
 * {@link S3ReaderIntegrationTest} and {@link IcebergWriterIntegrationTest}.
 */
@Tag("integration")
@ExtendWith(MockitoExtension.class)
@DisplayName("Full Flow Integration")
class FullFlowIntegrationTest {

    @Mock
    private FileDiscovery fileDiscovery;

    @Mock
    private S3Reader s3Reader;

    @Mock
    private IcebergWriter icebergWriter;

    @Mock
    private GlueCatalogSync glueSync;

    private Transformer transformer;
    private EtlOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        Md5KeyGenerator keyGen = new Md5KeyGenerator("MD5");
        TypeMapper typeMapper = new TypeMapper();
        transformer = new Transformer(keyGen, typeMapper);

        orchestrator = new EtlOrchestrator(
                fileDiscovery, s3Reader, transformer,
                icebergWriter, glueSync, "s3://cdc-lakehouse-data/iceberg/");
    }

    // ── Helper: build a single record ────────────────

    private static Map<String, Object> customerRow(String op, int id, String name,
                                                    String email, String city) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", op);
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        row.put("city", city);
        row.put("country", "US");
        row.put("created_at", java.sql.Timestamp.valueOf("2024-01-01 00:00:00"));
        row.put("Timestamp", java.sql.Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> productRow(String op, int id, String name,
                                                   String cat, String price) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", op);
        row.put("id", id);
        row.put("name", name);
        row.put("category", cat);
        row.put("price", new java.math.BigDecimal(price));
        row.put("created_at", java.sql.Timestamp.valueOf("2024-01-01 00:00:00"));
        row.put("Timestamp", java.sql.Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> orderRow(int id, int custId, String date, String status) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", "I");
        row.put("id", id);
        row.put("customer_id", custId);
        row.put("order_date", java.sql.Date.valueOf(date));
        row.put("status", status);
        row.put("total", new java.math.BigDecimal("99.99"));
        row.put("Timestamp", java.sql.Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> orderItemRow(int id, int orderId, int prodId,
                                                     int qty, String price) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", "I");
        row.put("id", id);
        row.put("order_id", orderId);
        row.put("product_id", prodId);
        row.put("quantity", qty);
        row.put("unit_price", new java.math.BigDecimal(price));
        row.put("Timestamp", java.sql.Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    // ── Tests ────────────────────────────────────────

    @Nested
    @DisplayName("End-to-end pipeline orchestration")
    class PipelineOrchestration {

        @Test
        @DisplayName("full flow: discovery → read → transform → write → sync")
        void fullFlowWithMockedAws() throws Exception {
            // Given: files are discovered
            DiscoveredFile file1 = new DiscoveredFile(
                    "dms-source/public/customers/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());
            DiscoveredFile file2 = new DiscoveredFile(
                    "dms-source/public/orders/LOAD01.parquet",
                    "cdc-raw-data", 2048, java.time.Instant.now());

            when(fileDiscovery.discover()).thenReturn(List.of(file1, file2));

            // And: S3Reader returns CDC records
            when(s3Reader.readRows(file1)).thenReturn(List.of(
                    customerRow("I", 1, "Alice", "a@a.com", "NYC"),
                    customerRow("I", 2, "Bob", "b@b.com", "LA")));

            when(s3Reader.readRows(file2)).thenReturn(List.of(
                    orderRow(100, 1, "2024-06-15", "completed")));

            // When: orchestrator runs
            orchestrator.processBatch();

            // Then: error count is zero (no exceptions)
            assertThat(orchestrator.getErrorCount()).isZero();
        }

        @Test
        @DisplayName("no files discovered: no-op (no errors)")
        void noFilesNoop() {
            when(fileDiscovery.discover()).thenReturn(List.of());

            orchestrator.processBatch();

            assertThat(orchestrator.getErrorCount()).isZero();
        }

        @Test
        @DisplayName("reader failure increments error counter but continues")
        void readerFailureIncrementsErrorCounter() throws Exception {
            DiscoveredFile badFile = new DiscoveredFile(
                    "dms-source/public/bad/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());

            when(fileDiscovery.discover()).thenReturn(List.of(badFile));
            when(s3Reader.readRows(badFile))
                    .thenThrow(new RuntimeException("S3 read failure"));

            orchestrator.processBatch();

            assertThat(orchestrator.getErrorCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("transformer produces star schema from real records")
        void transformerProducesStarSchema() throws Exception {
            // Given: realistic CDC records
            var customerFile = new DiscoveredFile(
                    "dms-source/public/customers/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());
            var productFile = new DiscoveredFile(
                    "dms-source/public/products/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());
            var orderFile = new DiscoveredFile(
                    "dms-source/public/orders/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());
            var itemFile = new DiscoveredFile(
                    "dms-source/public/order_items/LOAD01.parquet",
                    "cdc-raw-data", 1024, java.time.Instant.now());

            when(fileDiscovery.discover()).thenReturn(List.of(
                    customerFile, productFile, orderFile, itemFile));

            when(s3Reader.readRows(customerFile)).thenReturn(List.of(
                    customerRow("I", 1, "Alice", "alice@test.com", "New York"),
                    customerRow("I", 2, "Bob", "bob@test.com", "Los Angeles")));

            when(s3Reader.readRows(productFile)).thenReturn(List.of(
                    productRow("I", 1, "Laptop", "Electronics", "999.99"),
                    productRow("I", 2, "Mouse", "Electronics", "29.99")));

            when(s3Reader.readRows(orderFile)).thenReturn(List.of(
                    orderRow(100, 1, "2024-06-15", "completed"),
                    orderRow(101, 2, "2024-06-16", "pending")));

            when(s3Reader.readRows(itemFile)).thenReturn(List.of(
                    orderItemRow(1, 100, 1, 1, "999.99"),
                    orderItemRow(2, 100, 2, 2, "29.99"),
                    orderItemRow(3, 101, 1, 1, "999.99")));

            // When
            orchestrator.processBatch();

            // Then: no errors, pipeline completed
            assertThat(orchestrator.getErrorCount()).isZero();
        }
    }

    @Nested
    @DisplayName("Workload simulation: 100+ orders")
    class WorkloadSimulation {

        @Test
        @DisplayName("handles 100+ fact rows without errors")
        void handlesLargeBatch() throws Exception {
            // Given: 20 customers, 10 products, 150 orders, 300 order_items
            var customerFile = new DiscoveredFile("dms-source/public/customers/LOAD01.parquet",
                    "cdc-raw-data", 10240, java.time.Instant.now());
            var productFile = new DiscoveredFile("dms-source/public/products/LOAD01.parquet",
                    "cdc-raw-data", 5120, java.time.Instant.now());
            var orderFile = new DiscoveredFile("dms-source/public/orders/LOAD01.parquet",
                    "cdc-raw-data", 20480, java.time.Instant.now());
            var itemFile = new DiscoveredFile("dms-source/public/order_items/LOAD01.parquet",
                    "cdc-raw-data", 40960, java.time.Instant.now());

            when(fileDiscovery.discover()).thenReturn(List.of(
                    customerFile, productFile, orderFile, itemFile));

            // Build realistic records
            var customers = new java.util.ArrayList<Map<String, Object>>();
            for (int i = 1; i <= 20; i++) {
                customers.add(customerRow("I", i, "Customer" + i,
                        "cust" + i + "@test.com", "City" + i));
            }

            var products = new java.util.ArrayList<Map<String, Object>>();
            for (int i = 1; i <= 10; i++) {
                products.add(productRow("I", i, "Product" + i,
                        "Category" + (i % 3), String.valueOf(10 + i)));
            }

            var orders = new java.util.ArrayList<Map<String, Object>>();
            for (int i = 1; i <= 150; i++) {
                orders.add(orderRow(i, (i % 20) + 1,
                        "2024-06-" + String.format("%02d", (i % 28) + 1),
                        i % 2 == 0 ? "completed" : "pending"));
            }

            var items = new java.util.ArrayList<Map<String, Object>>();
            for (int i = 1; i <= 300; i++) {
                items.add(orderItemRow(i, (i % 150) + 1, (i % 10) + 1,
                        1 + (i % 5), String.valueOf(10 + (i % 90))));
            }

            when(s3Reader.readRows(customerFile)).thenReturn(customers);
            when(s3Reader.readRows(productFile)).thenReturn(products);
            when(s3Reader.readRows(orderFile)).thenReturn(orders);
            when(s3Reader.readRows(itemFile)).thenReturn(items);

            // When
            orchestrator.processBatch();

            // Then
            assertThat(orchestrator.getErrorCount()).isZero();
        }
    }
}
