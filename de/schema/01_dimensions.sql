-- =============================================================
-- 01_dimensions.sql
-- Snowflake Schema — Dimension Tables
-- Load order: this file FIRST, then 02_facts.sql
-- =============================================================

CREATE TABLE IF NOT EXISTS regions (
    region_id   SERIAL4      PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL,
    country     VARCHAR(100),
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id   SERIAL4      PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(200),
    region_id     INT4         REFERENCES regions(region_id),
    created_at    TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS branches (
    branch_id   SERIAL4      PRIMARY KEY,
    branch_name VARCHAR(150) NOT NULL,
    region_id   INT4         REFERENCES regions(region_id),
    address     TEXT,
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL4      PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email         VARCHAR(200),
    branch_id     INT4         REFERENCES branches(branch_id),
    joined_at     DATE,
    created_at    TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id       SERIAL4      PRIMARY KEY,
    product_name     VARCHAR(200) NOT NULL,
    category         VARCHAR(100),
    unit_cost        NUMERIC(10,2),
    created_at       TIMESTAMP    DEFAULT NOW()
);
