{{
    config(
        materialized = 'view', bind=False
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'CashTransactionIncremental2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'CashTransactionIncremental3') }}
