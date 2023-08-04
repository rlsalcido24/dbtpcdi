{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'TradeIncrementaldos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'TradeIncrementaltres') }}
