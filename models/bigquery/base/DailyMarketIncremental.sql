{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source(var('benchmark'), 'DailyMarketIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source(var('benchmark'), 'DailyMarketIncrementaltres') }}


