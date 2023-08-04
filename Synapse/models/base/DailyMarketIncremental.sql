{{
    config(
        materialized = 'view'
    )
}}


SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'DailyMarketIncremental2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'DailyMarketIncremental3') }}
