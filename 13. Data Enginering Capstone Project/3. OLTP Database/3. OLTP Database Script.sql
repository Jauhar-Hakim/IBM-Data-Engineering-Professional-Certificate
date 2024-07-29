CREATE DATABASE sales;

USE  sales;

SHOW DATABASES;

CREATE TABLE sales.sales_data(
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    price INT,
    quantity INT,
    timestamp DATETIME
);

SHOW TABLES;

SELECT table_name
FROM information_schema.tables
WHERE table_type='BASE TABLE'
      AND table_schema = 'sales';

CREATE INDEX ts ON sales_data(timestamp);

SHOW INDEX FROM sales.sales_data;

