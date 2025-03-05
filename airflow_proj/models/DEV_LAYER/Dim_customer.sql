{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('RAW_LAYER', 'CUSTOMER_TABLE') }}
    {% if is_incremental() %}
    -- Use LEFT JOIN instead of NOT IN for better performance
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} t WHERE t.customer_id = customer_table.customer_id
    )
    {% endif %}
),  

existing_data AS (
    -- Use {{ this }} to get existing records
    SELECT * FROM {{ this }}
),

changed_records AS (
    SELECT 
        s.index,
        s.customer_id,
        s.first_name,
        s.last_name,
        s.company,
        s.city,
        s.country,
        s.phone_1,
        s.phone_2,
        s.email,
        s.subscription_date,
        s.website
    FROM {{ source('RAW_LAYER', 'CUSTOMER_TABLE') }} s
    INNER JOIN existing_data e
    ON s.customer_id = e.customer_id
    WHERE 
        s.company <> e.company 
        OR s.city <> e.city 
        OR s.country <> e.country 
        OR s.email <> e.email
)

-- Insert both new and updated records
SELECT * FROM source_data
UNION ALL
SELECT * FROM changed_records
