{{
    config(
        materialized = 'view'
    )
}}

SELECT
    *,
    1 AS batchid
FROM
    {{ source('tpcdi', 'BatchDateuno') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'BatchDatedos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'BatchDatetres') }}
