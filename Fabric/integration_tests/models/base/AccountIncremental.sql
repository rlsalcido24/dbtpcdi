{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(accountid)'
    )
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'AccountIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'AccountIncremental3') }}
