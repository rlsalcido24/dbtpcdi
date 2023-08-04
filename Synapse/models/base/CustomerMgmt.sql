{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )
}}


SELECT *
FROM
    {{ source('tpcdi', 'CustomerMgmt') }}
