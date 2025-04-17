{{ config(materialized='table', schema='marts') }}

SELECT
    l.order_id,
    l.quantity,
    DATEDIFF(day, l.order_date, l.shipping_date) AS expected_duration,
    CASE 
        WHEN l.status = 'delivered' AND l.shipping_date > l.order_date + INTERVAL '5 days' THEN 1
        ELSE 0
    END AS is_late
FROM {{ ref('stg_logistics') }} l