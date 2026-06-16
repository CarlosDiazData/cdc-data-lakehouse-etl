-- =============================================================================
-- CDC Data Lakehouse — Athena Verification Queries
-- =============================================================================
-- Run against: lakehouse_db (Glue Data Catalog)
-- Prerequisites: Iceberg tables registered in Glue, data written by ETL processor
-- =============================================================================

-- ── 1. EXPLORE: Table schemas ────────────────────────────────────────────────

-- Verify all four star schema tables exist and are populated
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM lakehouse_db.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM lakehouse_db.dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM lakehouse_db.dim_date
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM lakehouse_db.fact_orders;


-- ── 2. Star Schema JOIN ──────────────────────────────────────────────────────
-- Denormalized e-commerce view: customer + product + order details

SELECT
    c.name           AS customer_name,
    c.email          AS customer_email,
    c.city           AS customer_city,
    p.product_name   AS product_name,
    p.category       AS product_category,
    d.full_date      AS order_date,
    d.day_of_week    AS order_day,
    f.quantity       AS quantity,
    f.unit_price     AS unit_price,
    f.total_amount   AS total_amount,
    f.order_status   AS order_status
FROM lakehouse_db.fact_orders f
JOIN lakehouse_db.dim_customer c ON f.customer_key = c.customer_key
JOIN lakehouse_db.dim_product  p ON f.product_key = p.product_key
JOIN lakehouse_db.dim_date     d ON f.date_sk = d.date_sk
WHERE c.is_current = true
ORDER BY f.total_amount DESC
LIMIT 20;


-- ── 3. AGGREGATION: Top customers by spend ───────────────────────────────────

SELECT
    c.customer_id,
    c.name          AS customer_name,
    c.email,
    COUNT(*)        AS order_count,
    SUM(f.total_amount) AS total_spent,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value
FROM lakehouse_db.fact_orders f
JOIN lakehouse_db.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 10;


-- ── 4. AGGREGATION: Top products by revenue ──────────────────────────────────

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(*)        AS units_sold,
    SUM(f.total_amount) AS total_revenue
FROM lakehouse_db.fact_orders f
JOIN lakehouse_db.dim_product p ON f.product_key = p.product_key
WHERE p.is_current = true
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- ── 5. AGGREGATION: Daily sales trend ────────────────────────────────────────

SELECT
    d.full_date,
    d.day_of_week,
    COUNT(*)        AS order_count,
    SUM(f.total_amount) AS daily_revenue
FROM lakehouse_db.fact_orders f
JOIN lakehouse_db.dim_date d ON f.date_sk = d.date_sk
GROUP BY d.full_date, d.day_of_week
ORDER BY d.full_date;


-- ── 6. AGGREGATION: Sales by quarter and category ────────────────────────────

SELECT
    d.year,
    d.quarter,
    p.category,
    COUNT(*)        AS transactions,
    SUM(f.total_amount) AS category_revenue
FROM lakehouse_db.fact_orders f
JOIN lakehouse_db.dim_date d ON f.date_sk = d.date_sk
JOIN lakehouse_db.dim_product p ON f.product_key = p.product_key
WHERE p.is_current = true
GROUP BY d.year, d.quarter, p.category
ORDER BY d.year, d.quarter, category_revenue DESC;


-- ── 7. DIMENSION QUALITY: Verify no orphan FK references ─────────────────────

-- Facts with missing customer references
SELECT COUNT(*) AS orphan_customer_facts
FROM lakehouse_db.fact_orders f
LEFT JOIN lakehouse_db.dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Facts with missing product references
SELECT COUNT(*) AS orphan_product_facts
FROM lakehouse_db.fact_orders f
LEFT JOIN lakehouse_db.dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Facts with missing date references
SELECT COUNT(*) AS orphan_date_facts
FROM lakehouse_db.fact_orders f
LEFT JOIN lakehouse_db.dim_date d ON f.date_sk = d.date_sk
WHERE d.date_sk IS NULL;


-- ── 8. SCD TYPE 2: Verify history tracking ───────────────────────────────────

-- Customers with multiple versions (SCD Type 2 history)
SELECT
    customer_id,
    COUNT(*)        AS version_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_rows
FROM lakehouse_db.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC;


-- ── 9. TIME TRAVEL: Point-in-time query ──────────────────────────────────────

-- Query fact_orders as they existed at a specific timestamp
-- Replace <timestamp> with a known snapshot time (e.g., before a batch update)
SELECT COUNT(*) AS fact_count_at_timestamp
FROM lakehouse_db."fact_orders"
FOR TIMESTAMP AS OF TIMESTAMP '<iso-8601-timestamp>';


-- ── 10. TIME TRAVEL: Version-based query ─────────────────────────────────────

-- Discover available snapshots for fact_orders
SELECT * FROM lakehouse_db."fact_orders$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- Query a specific snapshot by ID (replace <snapshot_id> with an actual value)
-- SELECT COUNT(*) AS fact_count_at_version
-- FROM lakehouse_db."fact_orders"
-- FOR SYSTEM_VERSION AS OF <snapshot_id>;


-- ── 11. SCHEMA EVOLUTION: Verify new columns are visible ─────────────────────

-- List all columns for each table to confirm schema
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'lakehouse_db'
  AND table_name = 'dim_customer'
ORDER BY ordinal_position;


-- ── 12. CONSISTENCY CHECK: Fact totals match line items ──────────────────────

-- Verify that fact_orders.total_amount = quantity × unit_price
SELECT COUNT(*) AS mismatched_totals
FROM lakehouse_db.fact_orders f
WHERE f.total_amount IS NOT NULL
  AND ABS(f.total_amount - (f.quantity * f.unit_price)) > 0.01;


-- ── 13. DEDUPLICATION VERIFY: No duplicate active customers ──────────────────

-- Each natural key should have at most one active (is_current=true) row
SELECT
    customer_id,
    COUNT(*) AS active_rows
FROM lakehouse_db.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- ── 14. END-TO-END SMOKE: Single customer order history ──────────────────────

-- Pick the first customer and show their complete order history
WITH first_customer AS (
    SELECT customer_key, customer_id, name
    FROM lakehouse_db.dim_customer
    WHERE is_current = true
    ORDER BY customer_id
    LIMIT 1
)
SELECT
    fc.name         AS customer_name,
    fc.customer_id,
    p.product_name,
    d.full_date     AS order_date,
    f.quantity,
    f.total_amount,
    f.order_status
FROM lakehouse_db.fact_orders f
JOIN first_customer fc ON f.customer_key = fc.customer_key
JOIN lakehouse_db.dim_product p ON f.product_key = p.product_key
JOIN lakehouse_db.dim_date d ON f.date_sk = d.date_sk
ORDER BY d.full_date;
