-- models/staging/stg_orders.sql
-- Clean and type-cast raw orders

{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    CAST(order_date   AS DATE)         AS order_date,
    LOWER(TRIM(status))                AS status,
    ROUND(total_amount::NUMERIC, 2)    AS total_amount,
    EXTRACT(YEAR  FROM order_date)     AS order_year,
    EXTRACT(MONTH FROM order_date)     AS order_month,
    EXTRACT(DOW   FROM order_date)     AS day_of_week,
    created_at
FROM {{ source('public', 'orders') }}
WHERE order_date IS NOT NULL
  AND order_id   IS NOT NULL
