package com.cdc.etl.transformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link Transformer}.
 *
 * <p>Validates Star Schema assembly, SCD Type 2 deduplication,
 * DELETE handling, and FK integrity.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Transformer")
class TransformerTest {

    @Mock
    private Md5KeyGenerator keyGen;

    @Mock
    private TypeMapper typeMapper;

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new Transformer(keyGen, typeMapper);
    }

    // ── Helpers ──────────────────────────────────────

    private static Map<String, Object> customerRec(String op, int id, String name,
                                                    String email, String city) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", op);
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        row.put("city", city);
        row.put("country", "US");
        row.put("created_at", Timestamp.valueOf("2024-01-01 00:00:00"));
        row.put("Timestamp", Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> productRec(String op, int id, String name,
                                                   String category, BigDecimal price) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", op);
        row.put("id", id);
        row.put("name", name);
        row.put("category", category);
        row.put("price", price);
        row.put("created_at", Timestamp.valueOf("2024-01-01 00:00:00"));
        row.put("Timestamp", Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> orderRec(int id, int customerId, LocalDate orderDate,
                                                 String status, BigDecimal total) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", "I");
        row.put("id", id);
        row.put("customer_id", customerId);
        row.put("order_date", java.sql.Date.valueOf(orderDate));
        row.put("status", status);
        row.put("total", total);
        row.put("Timestamp", Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private static Map<String, Object> orderItemRec(int id, int orderId, int productId,
                                                     int quantity, BigDecimal unitPrice) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("Op", "I");
        row.put("id", id);
        row.put("order_id", orderId);
        row.put("product_id", productId);
        row.put("quantity", quantity);
        row.put("unit_price", unitPrice);
        row.put("Timestamp", Timestamp.valueOf("2024-06-01 10:00:00"));
        return row;
    }

    private void mockKeyGen(String table, String naturalKey, long returnKey) {
        when(keyGen.generateKey(table, naturalKey)).thenReturn(returnKey);
    }

    private void mockKeyGen(String table, Object... naturalKeys) {
        // Accept any call matching the table name prefix
        when(keyGen.generateKey(anyString(), any())).thenAnswer(inv -> {
            String tbl = inv.getArgument(0);
            Object arg1 = inv.getArgument(1);
            if (arg1 instanceof String s) {
                return (long) (tbl + "|" + s).hashCode();
            }
            if (arg1 instanceof Integer i) {
                return (long) (tbl + "|" + i).hashCode();
            }
            return 42L;
        });
        when(keyGen.generateKey(anyString(), any(), any())).thenAnswer(inv -> {
            String tbl = inv.getArgument(0);
            Object a1 = inv.getArgument(1);
            Object a2 = inv.getArgument(2);
            return (long) (tbl + "|" + a1 + "|" + a2).hashCode();
        });
    }

    // ── Deduplication tests ──────────────────────────

    @Nested
    @DisplayName("CDC deduplication")
    class Deduplication {

        @Test
        @DisplayName("INSERT then UPDATE: keeps UPDATE")
        void insertThenUpdateKeepsUpdate() {
            Map<String, Object> insert = customerRec("I", 1, "Alice", "alice@old.com", "NYC");
            Map<String, Object> update = customerRec("U", 1, "Alice", "alice@new.com", "NYC");

            // UPDATE timestamp is newer
            update.put("Timestamp", Timestamp.valueOf("2024-06-02 10:00:00"));

            List<Map<String, Object>> deduped = transformer.deduplicate(
                    List.of(insert, update), "id");

            assertThat(deduped).hasSize(1);
            assertThat(deduped.getFirst().get("email")).isEqualTo("alice@new.com");
            assertThat(deduped.getFirst().get("Op")).isEqualTo("U");
        }

        @Test
        @DisplayName("UPDATE then INSERT: keeps UPDATE (higher priority)")
        void updateThenInsertKeepsUpdate() {
            Map<String, Object> update = customerRec("U", 1, "Alice", "alice@new.com", "NYC");
            Map<String, Object> insert = customerRec("I", 1, "Alice", "alice@old.com", "NYC");

            List<Map<String, Object>> deduped = transformer.deduplicate(
                    List.of(update, insert), "id");

            assertThat(deduped).hasSize(1);
            assertThat(deduped.getFirst().get("Op")).isEqualTo("U");
        }

        @Test
        @DisplayName("same Op: newer timestamp wins")
        void sameOpNewerTimestampWins() {
            Map<String, Object> older = customerRec("I", 1, "Alice", "alice@old.com", "NYC");
            Map<String, Object> newer = customerRec("I", 1, "Alice", "alice@new.com", "NYC");
            newer.put("Timestamp", Timestamp.valueOf("2024-06-03 10:00:00"));

            List<Map<String, Object>> deduped = transformer.deduplicate(
                    List.of(older, newer), "id");

            assertThat(deduped).hasSize(1);
            assertThat(deduped.getFirst().get("email")).isEqualTo("alice@new.com");
        }

        @Test
        @DisplayName("multiple distinct natural keys: all kept")
        void distinctKeysAllKept() {
            Map<String, Object> c1 = customerRec("I", 1, "Alice", "a@a.com", "NYC");
            Map<String, Object> c2 = customerRec("I", 2, "Bob", "b@b.com", "LA");
            Map<String, Object> c3 = customerRec("I", 3, "Carol", "c@c.com", "SF");

            List<Map<String, Object>> deduped = transformer.deduplicate(
                    List.of(c1, c2, c3), "id");

            assertThat(deduped).hasSize(3);
        }

        @Test
        @DisplayName("null PK skipped with warning")
        void nullPkSkipped() {
            Map<String, Object> valid = customerRec("I", 1, "Alice", "a@a.com", "NYC");
            Map<String, Object> missingPk = customerRec("I", 2, "Bob", "b@b.com", "LA");
            missingPk.remove("id");

            List<Map<String, Object>> deduped = transformer.deduplicate(
                    List.of(valid, missingPk), "id");

            assertThat(deduped).hasSize(1);
            assertThat(deduped.getFirst().get("name")).isEqualTo("Alice");
        }
    }

    // ── SCD Type 2 tests ─────────────────────────────

    @Nested
    @DisplayName("SCD Type 2 behavior")
    class ScdType2 {

        @BeforeEach
        void setUpMocks() {
            mockKeyGen("dim_customer", "1");
            mockKeyGen("dim_product", "1");
            mockKeyGen("dim_product", "2");
            // Also need to handle fact_orders composite keys
            when(keyGen.generateKey(anyString(), any(), any())).thenReturn(70000L);
        }

        @Test
        @DisplayName("INSERT creates row with isCurrent=true")
        void insertCreatesCurrentRow() {
            Map<String, Object> insert = customerRec("I", 1, "Alice", "a@a.com", "NYC");
            List<Map<String, Object>> records = List.of(insert);
            Map<String, List<Map<String, Object>>> byTable = Map.of("customers", records);

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.customers()).hasSize(1);
            StarSchemaModels.CustomerRow row = result.customers().getFirst();
            assertThat(row.isCurrent()).isTrue();
            assertThat(row.effectiveEndDate()).isNull();
            assertThat(row.customerId()).isEqualTo(1);
            assertThat(row.email()).isEqualTo("a@a.com");
        }

        @Test
        @DisplayName("DELETE creates row with isCurrent=false")
        void deleteSetsInactive() {
            Map<String, Object> delete = customerRec("D", 1, "Alice", "a@a.com", "NYC");
            List<Map<String, Object>> records = List.of(delete);
            Map<String, List<Map<String, Object>>> byTable = Map.of("customers", records);

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.customers()).hasSize(1);
            StarSchemaModels.CustomerRow row = result.customers().getFirst();
            assertThat(row.isCurrent()).isFalse();
        }

        @Test
        @DisplayName("product DELETE sets isCurrent=false in dim_product")
        void productDeleteSetsInactive() {
            Map<String, Object> delete = productRec("D", 1, "Widget", "Tools",
                    new BigDecimal("9.99"));
            List<Map<String, Object>> records = List.of(delete);
            Map<String, List<Map<String, Object>>> byTable = Map.of("products", records);

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.products()).hasSize(1);
            assertThat(result.products().getFirst().isCurrent()).isFalse();
        }
    }

    // ── Star Schema assembly ─────────────────────────

    @Nested
    @DisplayName("Star Schema assembly")
    class StarSchema {

        @BeforeEach
        void setUpMocks() {
            mockKeyGen("dim_customer", "1");
            mockKeyGen("dim_product", "1");
            mockKeyGen("dim_product", "2");
            when(keyGen.generateKey(anyString(), any(), any())).thenReturn(70000L);
        }

        @Test
        @DisplayName("full flow: customers + products + orders + order_items → star schema")
        void fullStarSchemaAssembly() {
            // Given: records from all 4 source tables
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();

            byTable.put("customers", List.of(
                    customerRec("I", 1, "Alice", "a@a.com", "NYC"),
                    customerRec("I", 2, "Bob", "b@b.com", "LA")));

            byTable.put("products", List.of(
                    productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99")),
                    productRec("I", 2, "Gadget", "Electronics", new BigDecimal("49.99"))));

            byTable.put("orders", List.of(
                    orderRec(100, 1, LocalDate.of(2024, 6, 15), "completed",
                            new BigDecimal("59.98"))));

            byTable.put("order_items", List.of(
                    orderItemRec(1001, 100, 1, 2, new BigDecimal("9.99")),
                    orderItemRec(1002, 100, 2, 1, new BigDecimal("49.99"))));

            // When
            TransformationResult result = transformer.transform(byTable);

            // Then
            assertThat(result.customers()).hasSize(2);
            assertThat(result.products()).hasSize(2);
            assertThat(result.dates()).isNotEmpty();  // at least the order date
            assertThat(result.facts()).hasSize(2);
        }

        @Test
        @DisplayName("fact rows reference valid FK keys")
        void factRowsHaveValidFks() {
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
            byTable.put("customers", List.of(customerRec("I", 1, "Alice", "a@a.com", "NYC")));
            byTable.put("products", List.of(productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99"))));
            byTable.put("orders", List.of(orderRec(100, 1, LocalDate.of(2024, 6, 15),
                    "completed", new BigDecimal("9.99"))));
            byTable.put("order_items", List.of(orderItemRec(1001, 100, 1, 1, new BigDecimal("9.99"))));

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.facts()).hasSize(1);
            StarSchemaModels.OrderFactRow fact = result.facts().getFirst();
            assertThat(fact.customerKey()).isNotZero();
            assertThat(fact.productKey()).isNotZero();
            assertThat(fact.dateSk()).isPositive();
            assertThat(fact.quantity()).isEqualTo(1);
            assertThat(fact.unitPrice()).isEqualByComparingTo(new BigDecimal("9.99"));
        }

        @Test
        @DisplayName("date dimension built from unique order dates")
        void dateDimensionUniqueDates() {
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
            byTable.put("customers", List.of(customerRec("I", 1, "Alice", "a@a.com", "NYC")));
            byTable.put("products", List.of(productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99"))));
            byTable.put("orders", List.of(
                    orderRec(100, 1, LocalDate.of(2024, 6, 15), "completed", new BigDecimal("10.00")),
                    orderRec(101, 1, LocalDate.of(2024, 6, 15), "pending", new BigDecimal("20.00")),
                    orderRec(102, 1, LocalDate.of(2024, 6, 16), "completed", new BigDecimal("30.00"))));
            byTable.put("order_items", List.of());

            TransformationResult result = transformer.transform(byTable);

            // Only 2 unique dates: 2024-06-15 and 2024-06-16
            assertThat(result.dates()).hasSize(2);
            assertThat(result.dates().stream().map(d -> d.fullDate().toString()).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("2024-06-15", "2024-06-16");
        }

        @Test
        @DisplayName("date dimension includes correct attributes")
        void dateDimensionAttributes() {
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
            byTable.put("customers", List.of(customerRec("I", 1, "Alice", "a@a.com", "NYC")));
            byTable.put("products", List.of(productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99"))));
            byTable.put("orders", List.of(orderRec(100, 1, LocalDate.of(2024, 1, 15),
                    "completed", new BigDecimal("10.00"))));
            byTable.put("order_items", List.of());

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.dates()).hasSize(1);
            StarSchemaModels.DateRow date = result.dates().getFirst();
            assertThat(date.dateSk()).isEqualTo(20240115);
            assertThat(date.year()).isEqualTo(2024);
            assertThat(date.month()).isEqualTo(1);
            assertThat(date.day()).isEqualTo(15);
            assertThat(date.quarter()).isEqualTo(1);
            assertThat(date.dayOfWeek()).isEqualTo("Monday");
            assertThat(date.isWeekend()).isFalse();
        }

        @Test
        @DisplayName("orphan order_item (no matching order) is skipped with warning")
        void orphanOrderItemSkipped() {
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
            byTable.put("customers", List.of(customerRec("I", 1, "Alice", "a@a.com", "NYC")));
            byTable.put("products", List.of(productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99"))));
            byTable.put("orders", List.of(orderRec(100, 1, LocalDate.of(2024, 6, 15),
                    "completed", new BigDecimal("10.00"))));
            // order_item references non-existent order_id 999
            byTable.put("order_items", List.of(orderItemRec(1001, 999, 1, 1, new BigDecimal("9.99"))));

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.facts()).isEmpty();
        }

        @Test
        @DisplayName("empty transformation returns empty result")
        void emptyResult() {
            TransformationResult result = transformer.transform(Map.of());

            assertThat(result.isEmpty()).isTrue();
            assertThat(result.customers()).isEmpty();
            assertThat(result.products()).isEmpty();
            assertThat(result.dates()).isEmpty();
            assertThat(result.facts()).isEmpty();
        }
    }

    // ── Fact order total calculation ─────────────────

    @Nested
    @DisplayName("Fact total calculation")
    class FactTotals {

        @BeforeEach
        void setUpMocks() {
            when(keyGen.generateKey(anyString(), any())).thenAnswer(inv -> {
                String table = inv.getArgument(0);
                String nk = String.valueOf(inv.getArgument(1));
                return (long) (table + "|" + nk).hashCode();
            });
            when(keyGen.generateKey(anyString(), any(), any())).thenReturn(70000L);
        }

        @Test
        @DisplayName("total_amount = quantity × unit_price")
        void totalIsQuantityTimesUnitPrice() {
            Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
            byTable.put("customers", List.of(customerRec("I", 1, "Alice", "a@a.com", "NYC")));
            byTable.put("products", List.of(productRec("I", 1, "Widget", "Tools", new BigDecimal("9.99"))));
            byTable.put("orders", List.of(orderRec(100, 1, LocalDate.of(2024, 6, 15),
                    "completed", new BigDecimal("49.95"))));
            byTable.put("order_items", List.of(orderItemRec(1001, 100, 1, 5, new BigDecimal("9.99"))));

            TransformationResult result = transformer.transform(byTable);

            assertThat(result.facts()).hasSize(1);
            StarSchemaModels.OrderFactRow fact = result.facts().getFirst();
            assertThat(fact.totalAmount()).isEqualByComparingTo(new BigDecimal("49.95"));
            assertThat(fact.quantity()).isEqualTo(5);
        }
    }

    // ── TransformationResult container ───────────────

    @Nested
    @DisplayName("TransformationResult")
    class ResultContainer {

        @Test
        @DisplayName("isEmpty returns true when all lists are empty")
        void isEmptyTrue() {
            var result = new TransformationResult(List.of(), List.of(), List.of(), List.of());
            assertThat(result.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("isEmpty returns false when any list has data")
        void isEmptyFalse() {
            var customer = new StarSchemaModels.CustomerRow(
                    1L, 1, "Alice", "a@a.com", "NYC", "US",
                    null, null, null, true);
            var result = new TransformationResult(List.of(customer), List.of(), List.of(), List.of());
            assertThat(result.isEmpty()).isFalse();
        }
    }
}
