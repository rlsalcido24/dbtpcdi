{{
    config(
        materialized = 'view'
    )
}}
SELECT
    *,
    1 AS batchid
FROM
    {{ source(var('benchmark'), 'DailyMarket') }}
