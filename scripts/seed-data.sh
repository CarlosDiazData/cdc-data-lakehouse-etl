#!/bin/bash
set -e

# RDS connection
HOST="cdclakehousestack-sourcedatabase70175d41-jx59cudu82oy.c65y66u4s2c3.us-east-1.rds.amazonaws.com"
PORT="5432"
DB="ecommerce"
USER="etl_user"
PASS='1b1dvze5gCtov^02KCmd_,fr,gFlQJ'

export PGPASSWORD="$PASS"

echo "=== Creating schema ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    city TEXT,
    country TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    order_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total NUMERIC(12,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL
);
SQL

echo "=== Truncating existing data ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "TRUNCATE order_items, orders, products, customers RESTART IDENTITY CASCADE;"

echo "=== Inserting customers ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<'SQL'
INSERT INTO customers (name, email, city, country) VALUES
('Juan Perez', 'juan@email.com', 'Buenos Aires', 'Argentina'),
('Maria Garcia', 'maria@email.com', 'Mexico City', 'Mexico'),
('Carlos Lopez', 'carlos@email.com', 'Madrid', 'Spain'),
('Ana Martinez', 'ana@email.com', 'Lima', 'Peru'),
('Pedro Rodriguez', 'pedro@email.com', 'Santiago', 'Chile'),
('Laura Fernandez', 'laura@email.com', 'Bogota', 'Colombia'),
('Diego Sanchez', 'diego@email.com', 'Montevideo', 'Uruguay'),
('Sofia Torres', 'sofia@email.com', 'Quito', 'Ecuador'),
('Mateo Ramirez', 'mateo@email.com', 'Caracas', 'Venezuela'),
('Valentina Diaz', 'valentina@email.com', 'Asuncion', 'Paraguay'),
('Sebastian Morales', 'sebastian@email.com', 'San Jose', 'Costa Rica'),
('Camila Herrera', 'camila@email.com', 'Panama City', 'Panama'),
('Nicolas Vargas', 'nicolas@email.com', 'Guatemala City', 'Guatemala'),
('Isabella Castro', 'isabella@email.com', 'Tegucigalpa', 'Honduras'),
('Daniel Mendoza', 'daniel@email.com', 'San Salvador', 'El Salvador'),
('Lucia Rojas', 'lucia@email.com', 'Managua', 'Nicaragua'),
('Alejandro Guzman', 'alejandro@email.com', 'Havana', 'Cuba'),
('Fernanda Reyes', 'fernanda@email.com', 'Santo Domingo', 'Dominican Republic'),
('Gabriel Ortiz', 'gabriel@email.com', 'La Paz', 'Bolivia');
SQL

echo "=== Inserting products ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<'SQL'
INSERT INTO products (name, category, price) VALUES
('Laptop Pro 15', 'Electronics', 1299.99),
('Wireless Mouse', 'Electronics', 29.99),
('USB-C Hub', 'Electronics', 49.99),
('Mechanical Keyboard', 'Electronics', 89.99),
('Monitor 27 4K', 'Electronics', 399.99),
('Standing Desk', 'Furniture', 549.99),
('Ergonomic Chair', 'Furniture', 299.99),
('Desk Lamp LED', 'Furniture', 39.99),
('Notebook A5', 'Office', 12.99),
('Pen Set Premium', 'Office', 24.99),
('Backpack Travel', 'Accessories', 79.99),
('Water Bottle Steel', 'Accessories', 19.99);
SQL

echo "=== Inserting orders ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<'SQL'
INSERT INTO orders (customer_id, order_date, status, total) VALUES
(1, CURRENT_DATE - 90, 'completed', 1329.98),
(2, CURRENT_DATE - 85, 'completed', 89.99),
(3, CURRENT_DATE - 80, 'completed', 549.99),
(4, CURRENT_DATE - 75, 'completed', 319.98),
(5, CURRENT_DATE - 70, 'completed', 449.98),
(6, CURRENT_DATE - 65, 'completed', 549.99),
(7, CURRENT_DATE - 60, 'completed', 89.99),
(8, CURRENT_DATE - 55, 'completed', 299.99),
(9, CURRENT_DATE - 50, 'completed', 39.99),
(10, CURRENT_DATE - 45, 'completed', 79.99),
(11, CURRENT_DATE - 40, 'completed', 1299.99),
(12, CURRENT_DATE - 35, 'completed', 49.99),
(13, CURRENT_DATE - 30, 'completed', 29.99),
(14, CURRENT_DATE - 25, 'completed', 399.99),
(15, CURRENT_DATE - 20, 'completed', 24.99),
(16, CURRENT_DATE - 15, 'completed', 19.99),
(17, CURRENT_DATE - 10, 'completed', 549.99),
(18, CURRENT_DATE - 5, 'completed', 299.99),
(19, CURRENT_DATE - 3, 'completed', 1299.99),
(1, CURRENT_DATE - 2, 'pending', 89.99),
(2, CURRENT_DATE - 1, 'pending', 399.99);
SQL

echo "=== Inserting order_items ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" <<'SQL'
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),
(1, 2, 1, 29.99),
(2, 4, 1, 89.99),
(3, 6, 1, 549.99),
(4, 7, 1, 299.99),
(4, 3, 1, 19.99),
(5, 5, 1, 399.99),
(5, 9, 1, 12.99),
(5, 10, 1, 24.99),
(6, 6, 1, 549.99),
(7, 4, 1, 89.99),
(8, 7, 1, 299.99),
(9, 8, 1, 39.99),
(10, 11, 1, 79.99),
(11, 1, 1, 1299.99),
(12, 3, 1, 49.99),
(13, 2, 1, 29.99),
(14, 5, 1, 399.99),
(15, 10, 1, 24.99),
(16, 12, 1, 19.99),
(17, 6, 1, 549.99),
(18, 7, 1, 299.99),
(19, 1, 1, 1299.99),
(20, 4, 1, 89.99),
(21, 5, 1, 399.99);
SQL

echo "=== Verifying data ==="
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "
SELECT 'customers' as table_name, COUNT(*) as rows FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items;
"

echo ""
echo "=== Seed data complete! ==="
