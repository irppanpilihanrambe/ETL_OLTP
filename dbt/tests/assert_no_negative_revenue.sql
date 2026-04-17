-- tests/assert_no_negative_revenue.sql
-- Fail if any order has negative revenue

SELECT order_id, total_amount
FROM {{ ref('stg_orders') }}
WHERE total_amount < 0
