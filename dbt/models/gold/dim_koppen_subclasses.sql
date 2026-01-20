{{ config(materialized='table') }}

SELECT
    code,
    class,
    name,
    description
FROM {{ ref('koppen_subclasses') }}