{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'DailyMarketIncrementaldos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'DailyMarketIncrementaltres') }}
