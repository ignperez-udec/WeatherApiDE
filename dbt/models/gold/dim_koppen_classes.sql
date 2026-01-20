{{ config(materialized='table') }}

SELECT
    class,
    name,
    description
FROM {{ ref('koppen_classes') }}