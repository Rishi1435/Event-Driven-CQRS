-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    items JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Seed initial products if they do not exist
INSERT INTO products (name, category, price)
SELECT 'Laptop', 'Electronics', 999.99
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'Laptop');

INSERT INTO products (name, category, price)
SELECT 'Smartphone', 'Electronics', 499.99
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'Smartphone');

INSERT INTO products (name, category, price)
SELECT 'Coffee Maker', 'Home & Kitchen', 89.99
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'Coffee Maker');

INSERT INTO products (name, category, price)
SELECT 'T-Shirt', 'Apparel', 19.99
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'T-Shirt');
