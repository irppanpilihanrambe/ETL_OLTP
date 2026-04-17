-- =============================================================
-- 03_indexes.sql
-- Performance indexes — run AFTER bulk COPY is complete
-- Phase 4 optimization
-- =============================================================

-- orders
CREATE INDEX IF NOT EXISTS idx_orders_customer_id  ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date    ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status        ON orders(status);

-- order_items (50M rows — critical)
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- daily_production
CREATE INDEX IF NOT EXISTS idx_daily_prod_date        ON daily_production(prod_date);
CREATE INDEX IF NOT EXISTS idx_daily_prod_supplier_id ON daily_production(supplier_id);
CREATE INDEX IF NOT EXISTS idx_daily_prod_product_id  ON daily_production(product_id);

-- customers
CREATE INDEX IF NOT EXISTS idx_customers_branch_id ON customers(branch_id);

-- Run ANALYZE after COPY + index creation
ANALYZE orders;
ANALYZE order_items;
ANALYZE daily_production;
ANALYZE customers;
