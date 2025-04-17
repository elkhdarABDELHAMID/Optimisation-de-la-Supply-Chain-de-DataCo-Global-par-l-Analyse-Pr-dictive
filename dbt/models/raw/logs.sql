{{ config(materialized='table', schema='raw') }}

SELECT
    CAST("timestamp" AS TIMESTAMP) AS access_time,
    CAST("user_id" AS STRING) AS user_id,
    CAST("action" AS STRING) AS action
FROM {{ source('external', 'logs_topic') }}