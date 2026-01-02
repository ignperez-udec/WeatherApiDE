{{ config(materialized='table', on_schema_change='ignore') }}

SELECT
    cod_location,
    location,
    province,
    region,
    latitude,
    longitude
FROM {{ source('silver', 'locations') }}