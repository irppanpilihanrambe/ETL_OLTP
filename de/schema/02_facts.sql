-- =============================================================
-- 02_facts.sql
-- Snowflake Schema — Fact Tables
-- Load order: run AFTER 01_dimensions.sql
-- =============================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL4      PRIMARY KEY,
    customer_id  INT4         REFERENCES customers(customer_id),
    order_date   DATE         NOT NULL,
    status       VARCHAR(50)  DEFAULT 'pending',
    total_amount NUMERIC(14,2),
    created_at   TIMESTAMP    DEFAULT NOW()
);

-- Partition candidate: 50M+ rows
-- Phase 4 will convert to RANGE partitioning by order_date
CREATE TABLE IF NOT EXISTS order_items (
    item_id     SERIAL4     PRIMARY KEY,
    order_id    INT4        REFERENCES orders(order_id),
    product_id  INT4        REFERENCES products(product_id),
    quantity    INT4        NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(10,2),
    created_at  TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_production (
    prod_id      SERIAL4   PRIMARY KEY,
    supplier_id  INT4      REFERENCES suppliers(supplier_id),
    product_id   INT4      REFERENCES products(product_id),
    prod_date    DATE      NOT NULL,
    qty_produced INT4      NOT NULL CHECK (qty_produced >= 0),
    created_at   TIMESTAMP DEFAULT NOW()
);
