{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )  
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'CustomerIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'CustomerIncremental3') }}
