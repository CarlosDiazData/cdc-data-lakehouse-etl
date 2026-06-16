package com.cdc.etl.seed;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Synthetic e-commerce data generator for the CDC Data Lakehouse pipeline.
 * <p>
 * Populates an RDS PostgreSQL instance with realistic seed data:
 * <ul>
 *   <li>20+ customers with plausible names and emails</li>
 *   <li>10+ products across multiple categories with prices</li>
 *   <li>100+ orders spanning a 90-day window with varied statuses</li>
 *   <li>300+ order_items linking orders to products</li>
 * </ul>
 * <p>
 * Supports {@code --force} flag for idempotent re-runs (truncates all tables first).
 * <p>
 * Usage:
 * <pre>
 * java SeedDataGenerator --db-url=jdbc:postgresql://host:5432/ecommerce --db-user=etl_user --db-pass=etl_pass
 * java SeedDataGenerator --db-url=... --force
 * </pre>
 */
public class SeedDataGenerator {

    // ── Data pools ────────────────────────────────────
    private static final List<String> FIRST_NAMES = Arrays.asList(
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy"
    );

    private static final List<String> LAST_NAMES = Arrays.asList(
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"
    );

    private static final List<String> CITIES = Arrays.asList(
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
            "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
            "Fort Worth", "Columbus", "Charlotte", "Indianapolis", "San Francisco", "Seattle"
    );

    private static final List<ProductTemplate> PRODUCT_TEMPLATES = Arrays.asList(
            new ProductTemplate("Wireless Headphones", "Electronics", 79.99),
            new ProductTemplate("USB-C Charging Cable", "Electronics", 12.99),
            new ProductTemplate("Laptop Stand", "Office", 45.50),
            new ProductTemplate("Mechanical Keyboard", "Electronics", 129.00),
            new ProductTemplate("Ergonomic Mouse", "Office", 34.99),
            new ProductTemplate("Coffee Mug Set", "Kitchen", 24.99),
            new ProductTemplate("Desk Lamp LED", "Office", 39.99),
            new ProductTemplate("Notebook Bundle", "Office", 18.50),
            new ProductTemplate("Water Bottle", "Sports", 15.99),
            new ProductTemplate("Yoga Mat", "Sports", 29.99),
            new ProductTemplate("Blender Pro", "Kitchen", 89.99),
            new ProductTemplate("Backpack 30L", "Sports", 59.99)
    );

    private static final List<String> ORDER_STATUSES = Arrays.asList(
            "pending", "shipped", "delivered", "cancelled"
    );

    // ── State ─────────────────────────────────────────
    private final String dbUrl;
    private final String dbUser;
    private final String dbPass;
    private final boolean force;
    private final Random rng = new Random();

    private final List<Long> customerIds = new ArrayList<>();
    private final List<Long> productIds = new ArrayList<>();
    private final List<Long> orderIds = new ArrayList<>();

    public SeedDataGenerator(String dbUrl, String dbUser, String dbPass, boolean force) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        this.force = force;
    }

    // ── Entry point ───────────────────────────────────
    public static void main(String[] args) {
        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;
        boolean force = false;

        for (String arg : args) {
            if (arg.startsWith("--db-url=")) {
                dbUrl = arg.substring("--db-url=".length());
            } else if (arg.startsWith("--db-user=")) {
                dbUser = arg.substring("--db-user=".length());
            } else if (arg.startsWith("--db-pass=")) {
                dbPass = arg.substring("--db-pass=".length());
            } else if ("--force".equals(arg)) {
                force = true;
            }
        }

        if (dbUrl == null) {
            System.err.println("ERROR: --db-url is required.");
            System.err.println("Usage: java SeedDataGenerator --db-url=jdbc:postgresql://host:5432/ecommerce --db-user=user --db-pass=pass [--force]");
            System.exit(1);
        }

        dbUser = dbUser != null ? dbUser : "postgres";
        dbPass = dbPass != null ? dbPass : "postgres";

        SeedDataGenerator generator = new SeedDataGenerator(dbUrl, dbUser, dbPass, force);
        try {
            generator.run();
            System.out.println("\n✅ Seed data generation completed successfully.");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("❌ Seed data generation failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    // ── Core runner ───────────────────────────────────
    public void run() throws Exception {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            conn.setAutoCommit(false);

            createSchema(conn);

            if (force) {
                truncateTables(conn);
                System.out.println("Truncated all tables (--force mode).");
            }

            generateCustomers(conn, 22);
            generateProducts(conn);
            generateOrders(conn, 110);
            generateOrderItems(conn, 330);

            conn.commit();
            printStats();
        }
    }

    // ── Schema DDL ────────────────────────────────────
    private void createSchema(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT        NOT NULL,
                    email       TEXT        NOT NULL,
                    city        TEXT        NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT          NOT NULL,
                    category    TEXT          NOT NULL,
                    price       NUMERIC(10,2) NOT NULL,
                    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id          SERIAL PRIMARY KEY,
                    customer_id INT           NOT NULL REFERENCES customers(id),
                    order_date  DATE          NOT NULL,
                    status      TEXT          NOT NULL,
                    total       NUMERIC(12,2) NOT NULL DEFAULT 0
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS order_items (
                    id          SERIAL PRIMARY KEY,
                    order_id    INT           NOT NULL REFERENCES orders(id),
                    product_id  INT           NOT NULL REFERENCES products(id),
                    quantity    INT           NOT NULL,
                    unit_price  NUMERIC(10,2) NOT NULL
                )
                """);

            System.out.println("Schema DDL applied (CREATE IF NOT EXISTS).");
        }
    }

    private void truncateTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE order_items, orders, products, customers RESTART IDENTITY CASCADE");
        }
    }

    // ── Data generation ───────────────────────────────
    private void generateCustomers(Connection conn, int count) throws SQLException {
        String sql = "INSERT INTO customers (name, email, city, created_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                String firstName = randomFrom(FIRST_NAMES);
                String lastName = randomFrom(LAST_NAMES);
                String name = firstName + " " + lastName;
                String email = (firstName.toLowerCase() + "." + lastName.toLowerCase() + i + "@example.com");
                String city = randomFrom(CITIES);
                Timestamp createdAt = randomTimestampLast30Days();

                ps.setString(1, name);
                ps.setString(2, email);
                ps.setString(3, city);
                ps.setTimestamp(4, createdAt);
                ps.executeUpdate();

                try (var rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        customerIds.add(rs.getLong(1));
                    }
                }
            }
        }
        System.out.printf("Inserted %d customers.%n", customerIds.size());
    }

    private void generateProducts(Connection conn) throws SQLException {
        String sql = "INSERT INTO products (name, category, price, created_at) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (ProductTemplate template : PRODUCT_TEMPLATES) {
                Timestamp createdAt = randomTimestampLast90Days();
                ps.setString(1, template.name);
                ps.setString(2, template.category);
                ps.setBigDecimal(3, BigDecimal.valueOf(template.price));
                ps.setTimestamp(4, createdAt);
                ps.executeUpdate();

                try (var rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        productIds.add(rs.getLong(1));
                    }
                }
            }
        }
        System.out.printf("Inserted %d products.%n", productIds.size());
    }

    private void generateOrders(Connection conn, int count) throws SQLException {
        String sql = "INSERT INTO orders (customer_id, order_date, status, total) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                long customerId = randomFrom(customerIds);
                LocalDate orderDate = randomDateLast90Days();
                String status = randomFromWeighted(ORDER_STATUSES,
                        new double[]{0.15, 0.25, 0.55, 0.05}); // weighted: mostly delivered/shipped
                BigDecimal total = BigDecimal.valueOf(randomPrice(0));
                ps.setLong(1, customerId);
                ps.setDate(2, java.sql.Date.valueOf(orderDate));
                ps.setString(3, status);
                ps.setBigDecimal(4, total);     // will be updated after items are generated
                ps.executeUpdate();

                try (var rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        orderIds.add(rs.getLong(1));
                    }
                }
            }
        }
        System.out.printf("Inserted %d orders.%n", orderIds.size());
    }

    private void generateOrderItems(Connection conn, int targetCount) throws SQLException {
        String sql = "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < targetCount; i++) {
                long orderId = randomFrom(orderIds);
                long productId = randomFrom(productIds);
                int quantity = rng.nextInt(1, 6);           // 1–5 items
                ProductTemplate product = PRODUCT_TEMPLATES.get(
                        productIds.indexOf(productId) % PRODUCT_TEMPLATES.size());
                BigDecimal unitPrice = BigDecimal.valueOf(product.price)
                        .setScale(2, RoundingMode.HALF_UP);

                ps.setLong(1, orderId);
                ps.setLong(2, productId);
                ps.setInt(3, quantity);
                ps.setBigDecimal(4, unitPrice);
                ps.executeUpdate();
            }
        }

        // Update order totals based on items
        updateOrderTotals(conn);
        System.out.printf("Inserted %d order_items.%n", targetCount);
    }

    private void updateOrderTotals(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                UPDATE orders o SET total = sub.total
                FROM (
                    SELECT order_id, SUM(quantity * unit_price) AS total
                    FROM order_items
                    GROUP BY order_id
                ) sub
                WHERE o.id = sub.order_id
                """);
        }
    }

    // ── Helpers ───────────────────────────────────────
    private <T> T randomFrom(List<T> list) {
        return list.get(rng.nextInt(list.size()));
    }

    private String randomFromWeighted(List<String> items, double[] weights) {
        double r = rng.nextDouble();
        double cumulative = 0.0;
        for (int i = 0; i < items.size(); i++) {
            cumulative += weights[i];
            if (r <= cumulative) {
                return items.get(i);
            }
        }
        return items.get(items.size() - 1);
    }

    private double randomPrice(double minPrice) {
        return Math.round((ThreadLocalRandom.current().nextDouble(5.0, 200.0)) * 100.0) / 100.0;
    }

    private LocalDate randomDateLast90Days() {
        LocalDate today = LocalDate.now();
        long daysBack = ThreadLocalRandom.current().nextLong(0, 90);
        return today.minusDays(daysBack);
    }

    private Timestamp randomTimestampLast30Days() {
        LocalDateTime now = LocalDateTime.now();
        long hoursBack = ThreadLocalRandom.current().nextLong(0, 30 * 24);
        return Timestamp.valueOf(now.minus(hoursBack, ChronoUnit.HOURS));
    }

    private Timestamp randomTimestampLast90Days() {
        LocalDateTime now = LocalDateTime.now();
        long hoursBack = ThreadLocalRandom.current().nextLong(0, 90 * 24);
        return Timestamp.valueOf(now.minus(hoursBack, ChronoUnit.HOURS));
    }

    private void printStats() {
        System.out.println("\n── Seed Data Statistics ──");
        System.out.printf("  Customers:    %d%n", customerIds.size());
        System.out.printf("  Products:     %d%n", productIds.size());
        System.out.printf("  Orders:       %d%n", orderIds.size());
        System.out.printf("  Order Items:  %d%n", 330);   // hardcoded target
        System.out.println("──────────────────────────");
    }

    // ── Inner types ───────────────────────────────────
    private record ProductTemplate(String name, String category, double price) {}
}
