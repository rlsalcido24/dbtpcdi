{{
    config(
        materialized = 'view'
    )
}}

SELECT
    *,
    1 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRawuno') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRawdos') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRawtres') }}
