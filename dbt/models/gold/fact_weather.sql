{{ config(
    materialized='incremental',
    unique_key='id',
    incrementak_strategy='merge',
    update_columns=[
        'weather_code',
        'temperature_mean',
        'temperature_max',
        'temperature_min',
        'sunrise',
        'sunset',
        'precipitation_sum',
        'rain_sum',
        'snowfall_sum',
        'precipitation_hours',
        'wind_direction_dominant',
        'shortwave_radiation_sum',
        'daylight_duration',
        'sunshine_duration',
        'relative_humidity_mean',
        'relative_humidity_max',
        'relative_humidity_min',
        'pressure_msl_mean',
        'pressure_msl_max',
        'pressure_msl_min',
        'cloud_cover_mean',
        'cloud_cover_max',
        'cloud_cover_min',
        'wind_speed_mean',
        'wind_speed_max',
        'wind_speed_min',
        'wind_gusts_mean',
        'wind_gusts_max',
        'wind_gusts_min',
        'updated_at'
    ]
    ) }}

{% set upsert_from = var('upsert_from', none) %}

SELECT
    MD5(CONCAT(wh.cod_location::TEXT,TO_CHAR(wh.time, 'YYYYMMDD'))) AS id,
    wh.cod_location,
    TO_CHAR(wh.time, 'YYYYMMDD')::BIGINT AS date_id,
    wh.weather_code,
    wh.temperature_mean,
    wh.temperature_max,
    wh.temperature_min,
    wh.sunrise,
    wh.sunset,
    wh.precipitation_sum,
    wh.rain_sum,
    wh.snowfall_sum,
    wh.precipitation_hours,
    wh.wind_direction_dominant,
    wh.shortwave_radiation_sum,
    wh.daylight_duration,
    wh.sunshine_duration,
    wh.relative_humidity_mean,
    wh.relative_humidity_max,
    wh.relative_humidity_min,
    wh.pressure_msl_mean,
    wh.pressure_msl_max,
    wh.pressure_msl_min,
    wh.cloud_cover_mean,
    wh.cloud_cover_max,
    wh.cloud_cover_min,
    wh.wind_speed_mean,
    wh.wind_speed_max,
    wh.wind_speed_min,
    wh.wind_gusts_mean,
    wh.wind_gusts_max,
    wh.wind_gusts_min,
    NOW() AS created_at,
    NOW() AS updated_at
FROM {{ source('silver','weather_hist') }} wh

{% if is_incremental() %}
    {% if upsert_from is not none %}
        WHERE wh.time >= '{{ upsert_from }}'
    {% endif %}
{% endif %}