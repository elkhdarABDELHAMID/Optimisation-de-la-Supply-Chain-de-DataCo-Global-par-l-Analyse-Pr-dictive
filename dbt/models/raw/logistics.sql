{{ config(materialized='table', schema='raw') }}

-- Simulation du chargement depuis Kafka vers Snowflake (remplacer par connecteur Kafka-Snowflake)
SELECT
    CAST("order_id" AS STRING) AS order_id,
    CAST("order_date" AS DATE) AS order_date,
    CAST("shipping_date" AS DATE) AS shipping_date,
    CAST("order_item_quantity" AS INT) AS quantity,
    CAST("warehouse" AS STRING) AS warehouse
FROM {{ source('external', 'logistics_topic') }}
