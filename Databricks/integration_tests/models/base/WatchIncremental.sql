{{
    config(
        materialized = 'view'
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'WatchIncrementaldos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'WatchIncrementaltres') }}
