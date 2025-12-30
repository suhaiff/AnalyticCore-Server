-- Sample SQL dump for testing dashboard import
-- Safe: CREATE TABLE + INSERT only

CREATE TABLE customers (
  id INT,
  name VARCHAR(100),
  country VARCHAR(50)
);

INSERT INTO customers (id, name, country) VALUES
(1, 'Alice', 'India'),
(2, 'Bob', 'USA'),
(3, 'Charlie', 'Germany');

CREATE TABLE orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  amount DECIMAL(10,2)
);

INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
(101, 1, '2024-01-05', 250.50),
(102, 2, '2024-01-06', 120.00),
(103, 1, '2024-01-07', 320.75),
(104, 3, '2024-01-08', 180.25);
