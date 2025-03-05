{{ config(
  materialized='table',
) }}

with country_count as (
    select country,count(*)as total_customers from {{ source('DEV_LAYER','DIM_CUSTOMER')}}
    group by country
)
select * from country_count