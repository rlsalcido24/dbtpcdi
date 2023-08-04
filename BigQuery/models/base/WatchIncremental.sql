{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'WatchIncrementaldos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'WatchIncrementaltres') }}
