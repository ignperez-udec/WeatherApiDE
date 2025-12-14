{{ config(materialized='table') }}

SELECT
    cod_location,
    location,
    province,
    region,
    latitude,
    longitude
FROM {{ source('silver', 'locations') }}