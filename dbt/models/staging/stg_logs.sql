{{ config(materialized='table', schema='staging') }}

SELECT
    access_time,
    user_id,
    action
FROM {{ ref('logs') }}
WHERE access_time IS NOT NULL