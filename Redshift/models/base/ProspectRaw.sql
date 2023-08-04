{{
    config(
        materialized = 'view', bind=False
    )
}}

SELECT
    *,
    1 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRaw1') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRaw2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'ProspectRaw3') }}
