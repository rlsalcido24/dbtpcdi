{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}



SELECT
    *,
    1 AS batchid
FROM
    {{ source('tpcdi', 'BatchDate1') }}

UNION ALL

SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'BatchDate2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'BatchDate3') }}
