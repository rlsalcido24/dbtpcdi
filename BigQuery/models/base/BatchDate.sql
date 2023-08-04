{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source(var('benchmark'), 'BatchDateuno') }}

union all

select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'BatchDatedos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'BatchDatetres') }}
