{{ config(materialized='table') }}

SELECT
    code,
    description
FROM {{ ref('weather_code_wmo') }}