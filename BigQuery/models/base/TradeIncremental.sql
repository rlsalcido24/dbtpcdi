{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source(var('benchmark'), 'TradeIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source(var('benchmark'), 'TradeIncrementaltres') }}


