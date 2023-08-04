{{
    config(
        materialized = 'view'
    )
}}
SELECT
    *,
    2 AS batchid
FROM
    {{ source(var('benchmark'), 'TradeIncrementaldos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source(var('benchmark'), 'TradeIncrementaltres') }}
