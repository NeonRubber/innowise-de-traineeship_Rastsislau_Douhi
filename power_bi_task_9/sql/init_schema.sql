-- 3-layer architecture for Superstore BI pipeline.

-- 1. Create schemas
CREATE SCHEMA IF NOT EXISTS layer_stage;
CREATE SCHEMA IF NOT EXISTS layer_core;
CREATE SCHEMA IF NOT EXISTS layer_mart;

-- LAYER_STAGE: Raw Ingestion
-- TEXT/VARCHAR for safe ingestion from CSV.
DROP TABLE IF EXISTS layer_stage.stg_superstore;
CREATE TABLE layer_stage.stg_superstore (
    row_id TEXT,
    order_id TEXT,
    order_date TEXT,
    ship_date TEXT,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    product_id TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales TEXT,
    quantity TEXT,
    discount TEXT,
    profit TEXT
);

-- LAYER_CORE: Normalized 3NF (History tracking)

-- Customers (SCD Type 2)
DROP TABLE IF EXISTS layer_core.dim_customers;
CREATE TABLE layer_core.dim_customers (
    customer_pk SERIAL PRIMARY KEY,
    customer_id TEXT, -- Business Key
    customer_name TEXT,
    segment TEXT,
    region TEXT, 
    _hash_diff TEXT,
    valid_from TIMESTAMP DEFAULT NOW(),
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Products (SCD Type 1)
DROP TABLE IF EXISTS layer_core.dim_products;
CREATE TABLE layer_core.dim_products (
    product_pk SERIAL PRIMARY KEY,
    product_id TEXT, -- Business Key
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    _hash_diff TEXT
);

-- Locations
DROP TABLE IF EXISTS layer_core.dim_locations;
CREATE TABLE layer_core.dim_locations (
    location_pk SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    _hash_diff TEXT
);

-- Fact Orders (Transaction level)
DROP TABLE IF EXISTS layer_core.fact_orders;
CREATE TABLE layer_core.fact_orders (
    order_pk SERIAL PRIMARY KEY,
    order_id TEXT, -- Part of composite key
    product_pk INT REFERENCES layer_core.dim_products(product_pk),
    customer_pk INT REFERENCES layer_core.dim_customers(customer_pk),
    location_pk INT REFERENCES layer_core.dim_locations(location_pk),
    order_date DATE,
    ship_date DATE,
    ship_mode TEXT,
    sales NUMERIC(15,2),
    quantity INT,
    discount NUMERIC(5,2),
    profit NUMERIC(15,2),
    _hash_diff TEXT
);

-- Mart views/tables will be refreshed from Core after ETL
DROP TABLE IF EXISTS layer_mart.dim_customer_mart;
CREATE TABLE layer_mart.dim_customer_mart (
    customer_pk INT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    region TEXT,
    is_current BOOLEAN
);

DROP TABLE IF EXISTS layer_mart.dim_product_mart;
CREATE TABLE layer_mart.dim_product_mart (
    product_pk INT,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    sub_category TEXT
);

DROP TABLE IF EXISTS layer_mart.fact_sales_mart;
CREATE TABLE layer_mart.fact_sales_mart (
    order_pk INT,
    order_id TEXT,
    product_pk INT,
    customer_pk INT,
    location_pk INT,
    order_date DATE,
    sales NUMERIC(15,2),
    quantity INT,
    profit NUMERIC(15,2)
);
