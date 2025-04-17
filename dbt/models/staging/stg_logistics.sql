{{ config(materialized='table', schema='staging') }}

SELECT
    order_id,
    order_date,
    shipping_date,
    quantity,
    warehouse,
    CASE 
        WHEN shipping_date > CURRENT_DATE THEN 'pending'
        ELSE 'delivered'
    END AS status
FROM {{ ref('logistics') }}
WHERE order_id IS NOT NULL
  AND quantity > 0