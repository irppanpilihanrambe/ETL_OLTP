-- models/analysis/analysis_customer_rfm.sql
-- RFM segmentation for all 200K customers

{{ config(materialized='table') }}

WITH rfm_base AS (
    SELECT
        o.customer_id,
        MAX(o.order_date)                              AS last_order_date,
        COUNT(DISTINCT o.order_id)                     AS frequency,
        SUM(oi.quantity * oi.unit_price)               AS monetary,
        CURRENT_DATE - MAX(o.order_date)               AS recency_days,
        AVG(oi.quantity * oi.unit_price)               AS avg_basket_size,
        STDDEV(oi.quantity * oi.unit_price)            AS basket_variance
    FROM {{ ref('stg_orders') }}                  o
    JOIN {{ source('public', 'order_items') }}    oi ON o.order_id = oi.order_id
    GROUP BY o.customer_id
),

rfm_scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)     AS m_score
    FROM rfm_base
)

SELECT
    customer_id,
    last_order_date,
    recency_days,
    frequency,
    ROUND(monetary::NUMERIC, 2)       AS lifetime_value,
    ROUND(avg_basket_size::NUMERIC, 2) AS avg_basket_size,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)     AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4              THEN 'Champion'
        WHEN r_score >= 3 AND f_score >= 3              THEN 'Loyal'
        WHEN r_score >= 3 AND f_score < 3               THEN 'Potential Loyal'
        WHEN r_score < 2  AND f_score >= 3              THEN 'At Risk'
        WHEN r_score < 2  AND f_score < 2               THEN 'Churned'
        ELSE 'Need Attention'
    END                                AS segment
FROM rfm_scored
