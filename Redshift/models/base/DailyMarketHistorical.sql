{{
    config(
        materialized = 'view', bind=False
    )
}}
SELECT
    *,
    1 AS batchid
FROM
    {{ source('tpcdi', 'DailyMarketHistorical') }}
