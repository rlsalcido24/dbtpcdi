{{
    config(
        materialized = 'view'
    )
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'TradeIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'TradeIncremental3') }}

