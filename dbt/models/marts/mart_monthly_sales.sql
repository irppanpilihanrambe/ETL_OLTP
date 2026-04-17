-- models/marts/mart_monthly_sales.sql
-- Monthly revenue by region and branch

{{ config(
    materialized = 'table',
    indexes      = [{'columns': ['month', 'region_name'], 'unique': false}]
) }}

SELECT
    r.region_name,
    b.branch_name,
    DATE_TRUNC('month', o.order_date)::DATE        AS month,
    COUNT(DISTINCT o.order_id)                      AS total_orders,
    SUM(oi.quantity * oi.unit_price)                AS total_revenue,
    AVG(oi.quantity * oi.unit_price)                AS avg_order_value,
    SUM(oi.quantity)                                AS total_units_sold,
    COUNT(DISTINCT o.customer_id)                   AS unique_customers
FROM {{ ref('stg_orders') }}          o
JOIN {{ source('public', 'order_items') }}  oi ON o.order_id    = oi.order_id
JOIN {{ source('public', 'customers') }}    c  ON o.customer_id = c.customer_id
JOIN {{ source('public', 'branches') }}     b  ON c.branch_id   = b.branch_id
JOIN {{ source('public', 'regions') }}      r  ON b.region_id   = r.region_id
GROUP BY r.region_name, b.branch_name, month
