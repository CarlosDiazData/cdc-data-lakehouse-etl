package com.cdc.etl.transformer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Domain model records for the Star Schema output.
 * <p>
 * These are intermediate representations produced by {@link Transformer}
 * and consumed by {@code IcebergWriter} for writing to Iceberg tables.
 */
public final class StarSchemaModels {

    private StarSchemaModels() { /* namespace only */ }

    /**
     * Dimension: Customer (SCD Type 2).
     * <p>
     * Each customer row has a surrogate key ({@code customerKey}) and a natural
     * key ({@code customerId}). When attributes change, a new row with a new
     * surrogate key is inserted and the old row's {@code isCurrent} is set to false.
     */
    public record CustomerRow(
            long customerKey,       // MD5 surrogate key
            int customerId,         // natural primary key from source
            String name,
            String email,
            String city,
            String country,
            java.sql.Timestamp createdDate,
            LocalDateTime effectiveStartDate,
            LocalDateTime effectiveEndDate,  // null = current
            boolean isCurrent
    ) {}

    /**
     * Dimension: Product (SCD Type 2).
     */
    public record ProductRow(
            long productKey,        // MD5 surrogate key
            int productId,          // natural primary key from source
            String productName,
            String category,
            BigDecimal unitPrice,
            java.sql.Timestamp createdDate,
            LocalDateTime effectiveStartDate,
            LocalDateTime effectiveEndDate,
            boolean isCurrent
    ) {}

    /**
     * Dimension: Date (static — generated once per date).
     */
    public record DateRow(
            int dateSk,             // YYYYMMDD as integer surrogate key
            java.sql.Date fullDate,
            int year,
            int month,
            int day,
            int quarter,
            String dayOfWeek,
            boolean isWeekend
    ) {}

    /**
     * Fact: Orders (transactional grain).
     */
    public record OrderFactRow(
            long orderSk,           // MD5 surrogate key
            int orderId,            // natural key from source
            long customerKey,       // FK → dim_customer
            long productKey,        // FK → dim_product
            int dateSk,             // FK → dim_date
            int quantity,
            BigDecimal unitPrice,
            BigDecimal totalAmount,
            String orderStatus,
            Timestamp orderTimestamp
    ) {}
}
