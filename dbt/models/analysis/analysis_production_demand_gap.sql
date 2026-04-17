-- models/analysis/analysis_production_demand_gap.sql
-- Daily gap: production output vs. order demand per product

{{ config(materialized='table') }}

WITH production AS (
    SELECT
        prod_date   AS ref_date,
        product_id,
        SUM(qty_produced) AS total_produced
    FROM {{ source('public', 'daily_production') }}
    GROUP BY prod_date, product_id
),

demand AS (
    SELECT
        o.order_date AS ref_date,
        oi.product_id,
        SUM(oi.quantity) AS total_demanded
    FROM {{ source('public', 'order_items') }}  oi
    JOIN {{ ref('stg_orders') }}                o  ON oi.order_id = o.order_id
    GROUP BY o.order_date, oi.product_id
)

SELECT
    COALESCE(p.ref_date, d.ref_date)          AS ref_date,
    COALESCE(p.product_id, d.product_id)      AS product_id,
    COALESCE(p.total_produced, 0)             AS total_produced,
    COALESCE(d.total_demanded, 0)             AS total_demanded,
    COALESCE(p.total_produced, 0) - COALESCE(d.total_demanded, 0) AS gap,
    CASE
        WHEN COALESCE(p.total_produced, 0) - COALESCE(d.total_demanded, 0) < 0
        THEN TRUE ELSE FALSE
    END AS is_shortfall
FROM production    p
FULL OUTER JOIN demand d
    ON  p.ref_date    = d.ref_date
    AND p.product_id  = d.product_id
ORDER BY ref_date DESC, gap ASC
