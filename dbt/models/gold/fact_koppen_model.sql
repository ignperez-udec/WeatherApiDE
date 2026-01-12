{{ config(materialized='table') }}

SELECT
    MD5(CONCAT(cod_location::TEXT, decade::TEXT)) AS id,
    cod_location,
    decade,
    t_min,
    t_max,
    t,
    p,
    t_mon10,
    p_driest,
    p_summer_driest,
    p_winter_driest,
    p_summer_wettest,
    p_winter_wettest,
    p_threshold,
    koppen_class,
    koppen_subclass
FROM {{ source('silver','koppen_model') }}