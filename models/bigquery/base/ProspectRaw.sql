{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{source(var('benchmark'), 'ProspectRawuno') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{source(var('benchmark'), 'ProspectRawdos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source(var('benchmark'), 'ProspectRawtres') }}
