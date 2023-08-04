{{
    config(
        materialized = 'view'
    )
}}

SELECT
    *,
    1 AS batchid
FROM
    {{ source(var('benchmark'), 'BatchDateuno') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source(var('benchmark'), 'BatchDatedos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source(var('benchmark'), 'BatchDatetres') }}
