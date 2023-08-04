{{
    config(
        materialized = 'view', bind=False
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'HoldingIncremental2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'HoldingIncremental3') }}
