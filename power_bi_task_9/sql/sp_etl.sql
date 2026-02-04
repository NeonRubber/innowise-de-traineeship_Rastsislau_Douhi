-- Stored procedures for Superstore BI pipeline.

-- 1. sp_load_stage: Load raw data from CSV to Stage
CREATE OR REPLACE PROCEDURE layer_stage.sp_load_stage(p_file_path TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE layer_stage.stg_superstore;
    
    EXECUTE format('COPY layer_stage.stg_superstore FROM %L WITH (FORMAT CSV, HEADER, ENCODING ''UTF8'')', p_file_path);
    
    RAISE NOTICE 'Stage table loaded from %', p_file_path;
END;
$$;

-- 2. sp_load_core_dimensions: Dim SCD logic
CREATE OR REPLACE PROCEDURE layer_core.sp_load_core_dimensions()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 2.1 Load Locations
    INSERT INTO layer_core.dim_locations (country, city, state, postal_code, _hash_diff)
    SELECT DISTINCT 
        country, city, state, postal_code,
        md5(concat(country, city, state, postal_code))
    FROM layer_stage.stg_superstore s
    WHERE NOT EXISTS (
        SELECT 1 FROM layer_core.dim_locations l 
        WHERE l._hash_diff = md5(concat(s.country, s.city, s.state, s.postal_code))
    );

    -- 2.2 Load Products (SCD Type 1)
    -- Update on hash mismatch
    UPDATE layer_core.dim_products p
    SET 
        product_name = t.product_name,
        category = t.category,
        sub_category = t.sub_category,
        _hash_diff = t.new_hash
    FROM (
        SELECT DISTINCT 
            product_id, product_name, category, sub_category,
            md5(concat(product_name, category, sub_category)) as new_hash
        FROM layer_stage.stg_superstore
    ) t
    WHERE p.product_id = t.product_id 
      AND p._hash_diff <> t.new_hash;

    -- Insert new products
    INSERT INTO layer_core.dim_products (product_id, product_name, category, sub_category, _hash_diff)
    SELECT DISTINCT 
        product_id, product_name, category, sub_category,
        md5(concat(product_name, category, sub_category))
    FROM layer_stage.stg_superstore s
    WHERE NOT EXISTS (
        SELECT 1 FROM layer_core.dim_products p WHERE p.product_id = s.product_id
    );

    -- 2.3 Load Customers (SCD Type 2)
    -- Close old versions
    UPDATE layer_core.dim_customers c
    SET 
        valid_to = NOW(),
        is_current = FALSE
    FROM (
        SELECT DISTINCT 
            customer_id, customer_name, segment, region,
            md5(concat(customer_name, segment, region)) as new_hash
        FROM layer_stage.stg_superstore
    ) t
    WHERE c.customer_id = t.customer_id 
      AND c.is_current = TRUE 
      AND c._hash_diff <> t.new_hash;

    -- Insert new versions
    INSERT INTO layer_core.dim_customers (
        customer_id, customer_name, segment, region, _hash_diff, 
        valid_from, valid_to, is_current
    )
    SELECT DISTINCT 
        customer_id, customer_name, segment, region,
        md5(concat(customer_name, segment, region)),
        NOW(),
        CAST(NULL AS TIMESTAMP),
        TRUE
    FROM layer_stage.stg_superstore s
    WHERE NOT EXISTS (
        SELECT 1 FROM layer_core.dim_customers c 
        WHERE c.customer_id = s.customer_id 
          AND c.is_current = TRUE 
          AND c._hash_diff = md5(concat(s.customer_name, s.segment, s.region))
    );

    RAISE NOTICE 'Dimensions loaded (SCD1 & SCD2 applied)';
END;
$$;

-- 3. sp_load_core_facts: Fact & Deduplication
CREATE OR REPLACE PROCEDURE layer_core.sp_load_core_facts()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO layer_core.fact_orders (
        order_id, product_pk, customer_pk, location_pk, 
        order_date, ship_date, ship_mode, 
        sales, quantity, discount, profit, _hash_diff
    )
    SELECT 
        s.order_id,
        p.product_pk,
        c.customer_pk,
        l.location_pk,
        CAST(s.order_date AS DATE), 
        CAST(s.ship_date AS DATE),
        s.ship_mode,
        CAST(REPLACE(s.sales, ',', '') AS NUMERIC),
        CAST(s.quantity AS INT),
        CAST(s.discount AS NUMERIC),
        CAST(REPLACE(s.profit, ',', '') AS NUMERIC),
        md5(concat(s.order_id, s.product_id, s.sales, s.quantity, s.order_date))
    FROM layer_stage.stg_superstore s
    LEFT JOIN layer_core.dim_products p ON s.product_id = p.product_id
    LEFT JOIN layer_core.dim_customers c ON s.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN layer_core.dim_locations l ON 
        l.postal_code = s.postal_code AND
        l.city = s.city AND
        l.state = s.state AND 
        l.country = s.country
    WHERE NOT EXISTS (
        -- Deduplication logic
        SELECT 1 FROM layer_core.fact_orders f 
        WHERE f.order_id = s.order_id AND f.product_pk = p.product_pk
    );

    RAISE NOTICE 'Fact orders loaded (Deduplicated & Safer Joins)';
END;
$$;

-- 4. sp_refresh_mart: Refresh Mart tables from Core
CREATE OR REPLACE PROCEDURE layer_mart.sp_refresh_mart()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refresh Customers
    TRUNCATE TABLE layer_mart.dim_customer_mart;
    INSERT INTO layer_mart.dim_customer_mart (customer_pk, customer_id, customer_name, segment, region, is_current)
    SELECT customer_pk, customer_id, customer_name, segment, region, is_current
    FROM layer_core.dim_customers;

    -- Refresh Products
    TRUNCATE TABLE layer_mart.dim_product_mart;
    INSERT INTO layer_mart.dim_product_mart (product_pk, product_id, product_name, category, sub_category)
    SELECT product_pk, product_id, product_name, category, sub_category
    FROM layer_core.dim_products;

    -- Refresh Sales
    TRUNCATE TABLE layer_mart.fact_sales_mart;
    INSERT INTO layer_mart.fact_sales_mart (order_pk, order_id, product_pk, customer_pk, location_pk, order_date, sales, quantity, profit)
    SELECT order_pk, order_id, product_pk, customer_pk, location_pk, order_date, sales, quantity, profit
    FROM layer_core.fact_orders;

    RAISE NOTICE 'Mart layer refreshed';
END;
$$;
