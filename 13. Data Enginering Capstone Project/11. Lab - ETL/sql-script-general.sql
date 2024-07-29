wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/sales.sql;

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/BFgMvlR8BKEjijGlWowK1Q/mysqlconnect.py;

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/postgresqlconnect.py;

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/automation.py;

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/sales-csv3mo8i5SHvta76u7DzUfhiw.csv;

CREATE DATABASE sales;

USE sales;

SOURCE sales.sql;

python3.11 -m pip install mysql-connector-python;
python3 -m pip install psycopg2;

python3.11 mysqlconnect.py;
python3 postgresqlconnect.py;

-- ===================================
CREATE SCHEMA sales
    AUTHORIZATION postgres;

CREATE TABLE sales.sales_data(
    rowid INT NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT,
    price decimal DEFAULT 0.0 NOT NULL,
    timeestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
    );

COPY sales.sales_data(rowid, product_id, customer_id, quantity, price, timeestamp)
FROM 'sales.csv'
DELIMITER ','
CSV HEADER;

--upload sales.csv

python3.11 automation.py