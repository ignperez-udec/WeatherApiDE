{{ config(materialized='table') }}

WITH
    date_series AS (
        SELECT generate_series('1940-01-01'::date, CURRENT_DATE, '1 day'::interval)::date AS date
    )

SELECT
    TO_CHAR(ds.date, 'YYYYMMDD')::BIGINT AS id,
    ds.date,
    EXTRACT(YEAR FROM ds.date) AS year,
    EXTRACT(MONTH FROM ds.date) AS month,
    EXTRACT(DAY FROM ds.date) AS day,
    TO_CHAR(ds.date, 'Day') AS day_name,
    CASE
        WHEN ds.date < s.autumn_equinox::DATE OR ds.date >= s.summer_solstice::DATE THEN 'Summer'
        WHEN ds.date BETWEEN s.autumn_equinox::DATE AND s.winter_solstice::DATE THEN 'Autumn'
        WHEN ds.date BETWEEN s.winter_solstice::DATE AND s.spring_equinox::DATE THEN 'Winter'
        WHEN ds.date BETWEEN s.spring_equinox::DATE AND s.summer_solstice::DATE THEN 'Spring'
    END AS season
FROM date_series ds
LEFT JOIN {{ source('silver','seasons') }} s ON
    EXTRACT(YEAR FROM ds.date) = s.year