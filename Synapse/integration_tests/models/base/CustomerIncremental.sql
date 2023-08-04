{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )  
}}


SELECT
    *,
    2 AS batchid
FROM
    {{ source('tpcdi', 'CustomerIncremental2') }}

UNION ALL

SELECT
    *,
    3 AS batchid
FROM
    {{ source('tpcdi', 'CustomerIncremental3') }}
