{{ config(materialized='table', schema='marts') }}

SELECT
    COUNT(*) AS total_orders,
    AVG(quantity) AS avg_quantity,
    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) / COUNT(*) * 100 AS on_time_delivery_rate
FROM {{ ref('stg_logistics') }}