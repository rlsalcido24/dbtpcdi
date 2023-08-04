{{
    config(
        materialized = 'view'
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source(var('benchmark'), 'HoldingIncrementaldos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source(var('benchmark'), 'HoldingIncrementaltres') }}
