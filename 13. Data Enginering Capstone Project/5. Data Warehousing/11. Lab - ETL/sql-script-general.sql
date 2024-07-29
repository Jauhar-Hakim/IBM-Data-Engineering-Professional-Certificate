CREATE DATABASE sales;

USE sales;

SOURCE sales.sql;

python3.11 -m pip install mysql-connector-python;

python3.11 mysqlconnect.py;

python3 -m pip install psycopg2;

python3 postgresqlconnect.py;

CREATE SCHEMA sales
    AUTHORIZATION postgres;

CREATE TABLE sales.sales_data(
    rowid INT NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT,
    price decimal DEFAULT 0.0 NOT NULL,
    timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
    )