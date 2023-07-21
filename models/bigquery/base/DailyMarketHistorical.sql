{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    1 as batchid
from
    {{source(var('benchmark'), 'DailyMarket') }}
