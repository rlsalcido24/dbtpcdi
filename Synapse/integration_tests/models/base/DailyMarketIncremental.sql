{{
    config(
        materialized = 'view'
    )
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncremental3') }}

