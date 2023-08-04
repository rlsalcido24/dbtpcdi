{{
    config(
        materialized = 'view'
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'DailyMarketIncrementaldos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'DailyMarketIncrementaltres') }}
