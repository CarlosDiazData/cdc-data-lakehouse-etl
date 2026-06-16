package com.cdc.etl.transformer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Assembles the Star Schema from raw CDC Parquet records.
 * <p>
 * Processing pipeline per batch:
 * <ol>
 *   <li>Deduplicate CDC events per natural key (Op column: UPDATE trumps INSERT)</li>
 *   <li>Handle DELETE events by marking dimensions inactive</li>
 *   <li>Map raw columns to typed dimension/fact records</li>
 *   <li>Generate MD5 surrogate keys for all rows</li>
 *   <li>Build dim_date entries for order dates</li>
 * </ol>
 * <p>
 * SCD Type 2 merge (expiring old rows, inserting new versions) is delegated
 * to the Iceberg writer, which reads current table state before writing.
 */
@Component
public class Transformer {

    private static final Logger log = LoggerFactory.getLogger(Transformer.class);

    /**
     * CDC Operation codes from DMS. Order: INSERT < UPDATE < DELETE.
     */
    private static final Map<String, Integer> OP_PRIORITY = Map.of(
            "I", 1,   // INSERT
            "U", 2,   // UPDATE
            "D", 3    // DELETE
    );

    private final Md5KeyGenerator keyGen;
    private final TypeMapper typeMapper;

    public Transformer(Md5KeyGenerator keyGen, TypeMapper typeMapper) {
        this.keyGen = keyGen;
        this.typeMapper = typeMapper;
    }

    /**
     * Transform raw CDC records grouped by source table into star schema rows.
     *
     * @param recordsByTable map of source-table-name → list of Parquet rows
     * @return assembled star schema rows
     */
    public TransformationResult transform(Map<String, List<Map<String, Object>>> recordsByTable) {
        log.info("Transforming CDC records from {} source tables", recordsByTable.size());

        List<StarSchemaModels.CustomerRow> customers = new ArrayList<>();
        List<StarSchemaModels.ProductRow> products = new ArrayList<>();
        List<StarSchemaModels.DateRow> dates = new ArrayList<>();
        List<StarSchemaModels.OrderFactRow> facts = new ArrayList<>();

        // Process each source table
        List<Map<String, Object>> customerRecords = deduplicate(
                recordsByTable.getOrDefault("customers", List.of()), "id");
        List<Map<String, Object>> productRecords = deduplicate(
                recordsByTable.getOrDefault("products", List.of()), "id");
        List<Map<String, Object>> orderRecords = deduplicate(
                recordsByTable.getOrDefault("orders", List.of()), "id");
        List<Map<String, Object>> orderItemRecords = deduplicate(
                recordsByTable.getOrDefault("order_items", List.of()), "id");

        // Build dimensions
        customers.addAll(buildCustomerDimension(customerRecords));
        products.addAll(buildProductDimension(productRecords));
        dates.addAll(buildDateDimension(orderRecords));

        // Build facts (joining order_items with orders)
        facts.addAll(buildFactOrders(orderRecords, orderItemRecords, customers, products, dates));

        log.info("Transformation complete: {} dim_customer, {} dim_product, {} dim_date, {} fact_orders",
                customers.size(), products.size(), dates.size(), facts.size());

        return new TransformationResult(customers, products, dates, facts);
    }

    // ── Deduplication ───────────────────────────────

    /**
     * Deduplicate CDC events: keep the latest record per natural key.
     * Rules: UPDATE > INSERT, DELETE handled separately.
     * Tie-break on Timestamp column (newer wins).
     */
    List<Map<String, Object>> deduplicate(List<Map<String, Object>> records, String pkColumn) {
        if (records.isEmpty()) return records;

        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();

        for (Map<String, Object> record : records) {
            Object key = record.get(pkColumn);
            if (key == null) {
                log.warn("Skipping record with null primary key '{}'", pkColumn);
                continue;
            }

            Map<String, Object> existing = latest.get(key);
            if (existing == null) {
                latest.put(key, record);
            } else {
                // Keep the higher-priority Op
                String newOp = stringValue(record.get("Op"));
                String existingOp = stringValue(existing.get("Op"));
                int newPriority = OP_PRIORITY.getOrDefault(newOp, 0);
                int existingPriority = OP_PRIORITY.getOrDefault(existingOp, 0);

                if (newPriority > existingPriority) {
                    latest.put(key, record);
                } else if (newPriority == existingPriority) {
                    // Tie-break on timestamp
                    Object newTs = record.get("Timestamp");
                    Object existingTs = existing.get("Timestamp");
                    if (newTs != null && existingTs != null) {
                        if (compareTimestamps(newTs, existingTs) > 0) {
                            latest.put(key, record);
                        }
                    }
                }
            }
        }

        return new ArrayList<>(latest.values());
    }

    // ── Dimension builders ──────────────────────────

    private List<StarSchemaModels.CustomerRow> buildCustomerDimension(
            List<Map<String, Object>> records) {

        List<StarSchemaModels.CustomerRow> rows = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        for (Map<String, Object> rec : records) {
            String op = stringValue(rec.get("Op"));
            int customerId = intValue(rec.get("id"));

            // For DELETE, produce an inactive row marker
            if ("D".equals(op)) {
                rows.add(new StarSchemaModels.CustomerRow(
                        keyGen.generateKey("dim_customer", String.valueOf(customerId)),
                        customerId,
                        stringValue(rec.get("name")),
                        stringValue(rec.get("email")),
                        stringValue(rec.get("city")),
                        stringValue(rec.get("country")),
                        timestampValue(rec.get("created_at")),
                        now, now, false));  // isCurrent = false
                continue;
            }

            // INSERT/UPDATE → active row
            rows.add(new StarSchemaModels.CustomerRow(
                    keyGen.generateKey("dim_customer", String.valueOf(customerId)),
                    customerId,
                    stringValue(rec.get("name")),
                    stringValue(rec.get("email")),
                    stringValue(rec.get("city")),
                    stringValue(rec.get("country")),
                    timestampValue(rec.get("created_at")),
                    now, null, true));  // isCurrent = true
        }

        return rows;
    }

    private List<StarSchemaModels.ProductRow> buildProductDimension(
            List<Map<String, Object>> records) {

        List<StarSchemaModels.ProductRow> rows = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        for (Map<String, Object> rec : records) {
            String op = stringValue(rec.get("Op"));
            int productId = intValue(rec.get("id"));

            if ("D".equals(op)) {
                rows.add(new StarSchemaModels.ProductRow(
                        keyGen.generateKey("dim_product", String.valueOf(productId)),
                        productId,
                        stringValue(rec.get("name")),
                        stringValue(rec.get("category")),
                        bigDecimalValue(rec.get("price")),
                        timestampValue(rec.get("created_at")),
                        now, now, false));
                continue;
            }

            rows.add(new StarSchemaModels.ProductRow(
                    keyGen.generateKey("dim_product", String.valueOf(productId)),
                    productId,
                    stringValue(rec.get("name")),
                    stringValue(rec.get("category")),
                    bigDecimalValue(rec.get("price")),
                    timestampValue(rec.get("created_at")),
                    now, null, true));
        }

        return rows;
    }

    private List<StarSchemaModels.DateRow> buildDateDimension(
            List<Map<String, Object>> orderRecords) {

        // Collect unique order dates
        var uniqueDates = orderRecords.stream()
                .map(r -> r.get("order_date"))
                .filter(d -> d != null)
                .map(Transformer::toLocalDate)
                .filter(d -> d != null)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<StarSchemaModels.DateRow> rows = new ArrayList<>();
        for (LocalDate date : uniqueDates) {
            int dateSk = date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
            DayOfWeek dow = date.getDayOfWeek();
            boolean isWeekend = dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY;

            rows.add(new StarSchemaModels.DateRow(
                    dateSk,
                    java.sql.Date.valueOf(date),
                    date.getYear(),
                    date.getMonthValue(),
                    date.getDayOfMonth(),
                    (date.getMonthValue() - 1) / 3 + 1,  // quarter
                    dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                    isWeekend));
        }

        return rows;
    }

    // ── Fact builder ────────────────────────────────

    private List<StarSchemaModels.OrderFactRow> buildFactOrders(
            List<Map<String, Object>> orderRecords,
            List<Map<String, Object>> orderItemRecords,
            List<StarSchemaModels.CustomerRow> customers,
            List<StarSchemaModels.ProductRow> products,
            List<StarSchemaModels.DateRow> dates) {

        // Build lookup maps
        Map<Integer, Long> customerKeyMap = customers.stream()
                .filter(StarSchemaModels.CustomerRow::isCurrent)
                .collect(Collectors.toMap(
                        StarSchemaModels.CustomerRow::customerId,
                        StarSchemaModels.CustomerRow::customerKey,
                        (a, b) -> b));

        Map<Integer, Long> productKeyMap = products.stream()
                .filter(StarSchemaModels.ProductRow::isCurrent)
                .collect(Collectors.toMap(
                        StarSchemaModels.ProductRow::productId,
                        StarSchemaModels.ProductRow::productKey,
                        (a, b) -> b));

        Map<LocalDate, Integer> dateKeyMap = dates.stream()
                .collect(Collectors.toMap(
                        r -> r.fullDate().toLocalDate(),
                        StarSchemaModels.DateRow::dateSk,
                        (a, b) -> b));

        // Build order index: order_id → order row
        Map<Integer, Map<String, Object>> orderIndex = orderRecords.stream()
                .collect(Collectors.toMap(
                        r -> intValue(r.get("id")),
                        r -> r,
                        (a, b) -> b));

        List<StarSchemaModels.OrderFactRow> facts = new ArrayList<>();

        for (Map<String, Object> item : orderItemRecords) {
            int orderId = intValue(item.get("order_id"));
            int productId = intValue(item.get("product_id"));
            Map<String, Object> order = orderIndex.get(orderId);

            if (order == null) {
                log.warn("Orphan order_item: order_id={} not found in orders", orderId);
                continue;
            }

            Long customerKey = customerKeyMap.get(intValue(order.get("customer_id")));
            Long productKey = productKeyMap.get(productId);
            Integer dateSk = dateKeyMap.get(toLocalDate(order.get("order_date")));

            if (customerKey == null || productKey == null || dateSk == null) {
                log.warn("Missing FK for order_item: order_id={}, product_id={}", orderId, productId);
                continue;
            }

            int quantity = intValue(item.get("quantity"));
            BigDecimal unitPrice = bigDecimalValue(item.get("unit_price"));
            BigDecimal totalAmount = unitPrice.multiply(BigDecimal.valueOf(quantity));

            facts.add(new StarSchemaModels.OrderFactRow(
                    keyGen.generateKey("fact_orders", orderId, productId),
                    orderId,
                    customerKey,
                    productKey,
                    dateSk,
                    quantity,
                    unitPrice,
                    totalAmount,
                    stringValue(order.get("status")),
                    timestampValue(order.get("order_date"))));
        }

        return facts;
    }

    // ── Type-safe extractors ────────────────────────

    private static String stringValue(Object value) {
        if (value == null) return null;
        return value.toString();
    }

    private static int intValue(Object value) {
        if (value == null) return 0;
        if (value instanceof Number n) return n.intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static BigDecimal bigDecimalValue(Object value) {
        if (value == null) return BigDecimal.ZERO;
        if (value instanceof BigDecimal bd) return bd;
        if (value instanceof Number n) return BigDecimal.valueOf(n.doubleValue());
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    private static Timestamp timestampValue(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp ts) return ts;
        if (value instanceof java.util.Date d) return new Timestamp(d.getTime());
        if (value instanceof LocalDateTime ldt) return Timestamp.valueOf(ldt);
        if (value instanceof LocalDate ld) return Timestamp.valueOf(ld.atStartOfDay());
        return null;
    }

    private static LocalDate toLocalDate(Object value) {
        if (value == null) return null;
        if (value instanceof LocalDate ld) return ld;
        if (value instanceof java.sql.Date d) return d.toLocalDate();
        if (value instanceof Timestamp ts) return ts.toLocalDateTime().toLocalDate();
        if (value instanceof java.util.Date d) return d.toInstant().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        return null;
    }

    private static int compareTimestamps(Object a, Object b) {
        Timestamp ta = timestampValue(a);
        Timestamp tb = timestampValue(b);
        if (ta == null && tb == null) return 0;
        if (ta == null) return -1;
        if (tb == null) return 1;
        return ta.compareTo(tb);
    }
}
