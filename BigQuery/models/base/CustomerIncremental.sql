{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source(var('benchmark'), 'customerincrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source(var('benchmark'), 'customerincrementaltres') }}

