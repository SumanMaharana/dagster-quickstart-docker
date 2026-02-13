-- 02-seed-data.sql
-- Populate tables with realistic relational data

-- ============================================
-- SEED CUSTOMERS
-- ============================================
INSERT INTO source.customers (first_name, last_name, email, phone, address, city, state, zip_code, country) VALUES
('John', 'Smith', 'john.smith@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', 'USA'),
('Emma', 'Johnson', 'emma.j@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'USA'),
('Michael', 'Williams', 'michael.w@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA'),
('Sarah', 'Brown', 'sarah.brown@email.com', '555-0104', '321 Elm St', 'Houston', 'TX', '77001', 'USA'),
('David', 'Jones', 'david.jones@email.com', '555-0105', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'USA'),
('Lisa', 'Garcia', 'lisa.garcia@email.com', '555-0106', '987 Cedar Ln', 'Philadelphia', 'PA', '19101', 'USA'),
('James', 'Martinez', 'james.m@email.com', '555-0107', '147 Birch Ct', 'San Antonio', 'TX', '78201', 'USA'),
('Jennifer', 'Rodriguez', 'jennifer.r@email.com', '555-0108', '258 Walnut Pl', 'San Diego', 'CA', '92101', 'USA'),
('Robert', 'Davis', 'robert.davis@email.com', '555-0109', '369 Spruce Way', 'Dallas', 'TX', '75201', 'USA'),
('Maria', 'Wilson', 'maria.wilson@email.com', '555-0110', '741 Ash Blvd', 'San Jose', 'CA', '95101', 'USA'),
('William', 'Anderson', 'william.a@email.com', '555-0111', '852 Cherry St', 'Austin', 'TX', '73301', 'USA'),
('Jessica', 'Taylor', 'jessica.t@email.com', '555-0112', '963 Poplar Ave', 'Jacksonville', 'FL', '32099', 'USA'),
('Christopher', 'Thomas', 'chris.thomas@email.com', '555-0113', '159 Willow Rd', 'Fort Worth', 'TX', '76101', 'USA'),
('Ashley', 'Moore', 'ashley.moore@email.com', '555-0114', '357 Dogwood Dr', 'Columbus', 'OH', '43004', 'USA'),
('Daniel', 'Jackson', 'daniel.j@email.com', '555-0115', '753 Magnolia Ln', 'Charlotte', 'NC', '28201', 'USA');

-- ============================================
-- SEED PRODUCTS
-- ============================================
INSERT INTO source.products (product_name, category, price, stock_quantity, supplier, description) VALUES
-- Electronics
('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'TechCorp Inc', 'High-performance laptop with 16GB RAM'),
('Wireless Mouse', 'Electronics', 29.99, 200, 'TechCorp Inc', 'Ergonomic wireless mouse with USB receiver'),
('USB-C Hub', 'Electronics', 49.99, 150, 'TechCorp Inc', '7-in-1 USB-C hub with HDMI and Ethernet'),
('Bluetooth Headphones', 'Electronics', 149.99, 100, 'AudioMax Ltd', 'Noise-cancelling over-ear headphones'),
('4K Monitor', 'Electronics', 399.99, 75, 'TechCorp Inc', '27-inch 4K IPS display'),

-- Office Supplies
('Office Chair', 'Furniture', 249.99, 30, 'ErgoFurniture Co', 'Ergonomic office chair with lumbar support'),
('Standing Desk', 'Furniture', 499.99, 20, 'ErgoFurniture Co', 'Electric height-adjustable standing desk'),
('Desk Lamp', 'Furniture', 39.99, 100, 'LightUp Inc', 'LED desk lamp with adjustable brightness'),
('File Cabinet', 'Furniture', 129.99, 40, 'ErgoFurniture Co', '3-drawer file cabinet with lock'),

-- Stationery
('Notebook Set', 'Stationery', 19.99, 300, 'Paper Plus', 'Pack of 5 ruled notebooks'),
('Pen Set', 'Stationery', 14.99, 250, 'WriteWell Corp', 'Set of 12 ballpoint pens'),
('Sticky Notes', 'Stationery', 9.99, 400, 'Paper Plus', 'Assorted colors, pack of 6'),
('Desk Organizer', 'Stationery', 24.99, 150, 'OrganizePro', 'Bamboo desk organizer with compartments'),

-- Books
('Python Programming Guide', 'Books', 44.99, 80, 'BookWorld', 'Comprehensive Python programming book'),
('Data Science Handbook', 'Books', 54.99, 60, 'BookWorld', 'Essential guide to data science'),
('Project Management 101', 'Books', 34.99, 90, 'BookWorld', 'Introduction to project management');

-- ============================================
-- SEED ORDERS
-- ============================================
INSERT INTO source.orders (customer_id, order_date, total_amount, status, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method) VALUES
-- Recent orders (last 30 days)
(1, CURRENT_TIMESTAMP - INTERVAL '2 days', 1329.98, 'processing', '123 Main St', 'New York', 'NY', '10001', 'Credit Card'),
(2, CURRENT_TIMESTAMP - INTERVAL '5 days', 449.97, 'shipped', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'PayPal'),
(3, CURRENT_TIMESTAMP - INTERVAL '7 days', 179.98, 'delivered', '789 Pine Rd', 'Chicago', 'IL', '60601', 'Credit Card'),
(4, CURRENT_TIMESTAMP - INTERVAL '10 days', 749.98, 'delivered', '321 Elm St', 'Houston', 'TX', '77001', 'Credit Card'),
(5, CURRENT_TIMESTAMP - INTERVAL '12 days', 59.98, 'delivered', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'Debit Card'),

-- Older orders (31-60 days)
(6, CURRENT_TIMESTAMP - INTERVAL '35 days', 399.99, 'delivered', '987 Cedar Ln', 'Philadelphia', 'PA', '19101', 'Credit Card'),
(7, CURRENT_TIMESTAMP - INTERVAL '40 days', 529.98, 'delivered', '147 Birch Ct', 'San Antonio', 'TX', '78201', 'PayPal'),
(8, CURRENT_TIMESTAMP - INTERVAL '45 days', 94.97, 'delivered', '258 Walnut Pl', 'San Diego', 'CA', '92101', 'Credit Card'),
(9, CURRENT_TIMESTAMP - INTERVAL '50 days', 1549.97, 'delivered', '369 Spruce Way', 'Dallas', 'TX', '75201', 'Credit Card'),
(10, CURRENT_TIMESTAMP - INTERVAL '55 days', 249.99, 'delivered', '741 Ash Blvd', 'San Jose', 'CA', '95101', 'Debit Card'),

-- Older orders (61-90 days)
(1, CURRENT_TIMESTAMP - INTERVAL '65 days', 79.98, 'delivered', '123 Main St', 'New York', 'NY', '10001', 'Credit Card'),
(3, CURRENT_TIMESTAMP - INTERVAL '70 days', 399.99, 'delivered', '789 Pine Rd', 'Chicago', 'IL', '60601', 'PayPal'),
(5, CURRENT_TIMESTAMP - INTERVAL '75 days', 149.99, 'delivered', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'Credit Card'),
(11, CURRENT_TIMESTAMP - INTERVAL '80 days', 499.99, 'delivered', '852 Cherry St', 'Austin', 'TX', '73301', 'Credit Card'),
(12, CURRENT_TIMESTAMP - INTERVAL '85 days', 89.97, 'delivered', '963 Poplar Ave', 'Jacksonville', 'FL', '32099', 'Debit Card');

-- ============================================
-- SEED ORDER_ITEMS
-- ============================================
-- Order 1 (customer 1, recent)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 1299.99, 1299.99),
(1, 2, 1, 29.99, 29.99);

-- Order 2 (customer 2)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(2, 5, 1, 399.99, 399.99),
(2, 3, 1, 49.99, 49.99);

-- Order 3 (customer 3)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(3, 4, 1, 149.99, 149.99),
(3, 2, 1, 29.99, 29.99);

-- Order 4 (customer 4)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(4, 6, 1, 249.99, 249.99),
(4, 7, 1, 499.99, 499.99);

-- Order 5 (customer 5)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(5, 10, 2, 19.99, 39.98),
(5, 11, 1, 14.99, 14.99),
(5, 12, 1, 9.99, 9.99);

-- Order 6 (customer 6)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(6, 5, 1, 399.99, 399.99);

-- Order 7 (customer 7)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(7, 6, 1, 249.99, 249.99),
(7, 8, 1, 39.99, 39.99),
(7, 9, 1, 129.99, 129.99),
(7, 10, 1, 19.99, 19.99),
(7, 13, 4, 24.99, 99.96);

-- Order 8 (customer 8)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(8, 14, 1, 44.99, 44.99),
(8, 15, 1, 54.99, 54.99);

-- Order 9 (customer 9)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(9, 1, 1, 1299.99, 1299.99),
(9, 6, 1, 249.99, 249.99);

-- Order 10 (customer 10)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(10, 6, 1, 249.99, 249.99);

-- Order 11 (customer 1, older)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(11, 2, 2, 29.99, 59.98),
(11, 11, 1, 14.99, 14.99);

-- Order 12 (customer 3, older)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(12, 5, 1, 399.99, 399.99);

-- Order 13 (customer 5, older)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(13, 4, 1, 149.99, 149.99);

-- Order 14 (customer 11)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(14, 7, 1, 499.99, 499.99);

-- Order 15 (customer 12)
INSERT INTO source.order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(15, 14, 1, 44.99, 44.99),
(15, 15, 1, 54.99, 54.99);

-- ============================================
-- VERIFY DATA
-- ============================================
DO $$
DECLARE
    customer_count INTEGER;
    product_count INTEGER;
    order_count INTEGER;
    order_item_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO customer_count FROM source.customers;
    SELECT COUNT(*) INTO product_count FROM source.products;
    SELECT COUNT(*) INTO order_count FROM source.orders;
    SELECT COUNT(*) INTO order_item_count FROM source.order_items;
    
    RAISE NOTICE 'Data seeded successfully:';
    RAISE NOTICE '  - Customers: %', customer_count;
    RAISE NOTICE '  - Products: %', product_count;
    RAISE NOTICE '  - Orders: %', order_count;
    RAISE NOTICE '  - Order Items: %', order_item_count;
END $$;

