-- 01-create-schemas.sql
-- Create schemas and tables with foreign key relationships for realistic data lineage
-- This database contains: Dagster system (public schema), source data (source schema), warehouse data (warehouse schema)

-- ============================================
-- CREATE SCHEMAS
-- ============================================
CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================
-- SOURCE SCHEMA TABLES
-- ============================================

-- CUSTOMERS TABLE
CREATE TABLE IF NOT EXISTS source.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on email for faster lookups
CREATE INDEX IF NOT EXISTS idx_customers_email ON source.customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_name ON source.customers(last_name, first_name);

-- PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS source.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    supplier VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_products_category ON source.products(category);
CREATE INDEX IF NOT EXISTS idx_products_name ON source.products(product_name);

-- ORDERS TABLE (with foreign keys)
CREATE TABLE IF NOT EXISTS source.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    shipping_address VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_state VARCHAR(50),
    shipping_zip VARCHAR(20),
    payment_method VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer
        FOREIGN KEY(customer_id)
        REFERENCES source.customers(customer_id)
        ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_orders_customer ON source.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON source.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON source.orders(status);

-- ORDER_ITEMS TABLE (with foreign keys to orders and products)
CREATE TABLE IF NOT EXISTS source.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price DECIMAL(10, 2) NOT NULL CHECK (total_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order
        FOREIGN KEY(order_id)
        REFERENCES source.orders(order_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
        REFERENCES source.products(product_id)
        ON DELETE RESTRICT
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_order_items_order ON source.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON source.order_items(product_id);

-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================

-- Customer summary view
CREATE OR REPLACE VIEW source.customer_order_summary AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date
FROM source.customers c
LEFT JOIN source.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

-- Product sales summary view
CREATE OR REPLACE VIEW source.product_sales_summary AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    COUNT(oi.order_item_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.total_price) as total_revenue
FROM source.products p
LEFT JOIN source.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category, p.price;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Schema created successfully with tables: customers, products, orders, order_items';
    RAISE NOTICE 'Views created: customer_order_summary, product_sales_summary';
END $$;
