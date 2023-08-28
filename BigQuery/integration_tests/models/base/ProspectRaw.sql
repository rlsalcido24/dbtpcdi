{{
    config(
        materialized = 'view'
    )
}}

SELECT
    *,
    1 AS batchid
FROM
    {{ source(var('benchmark'), 'ProspectRawuno') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source(var('benchmark'), 'ProspectRawdos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source(var('benchmark'), 'ProspectRawtres') }}
