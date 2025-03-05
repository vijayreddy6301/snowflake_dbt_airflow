{{ config(
    materialized='view'
   
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('RAW_LAYER', 'CUSTOMER_TABLE') }}
)
SELECT * FROM source_data
