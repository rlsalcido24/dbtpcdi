{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{source(var('benchmark'), 'BatchDateuno') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{source(var('benchmark'), 'BatchDatedos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source(var('benchmark'), 'BatchDatetres') }}

