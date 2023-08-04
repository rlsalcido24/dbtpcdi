{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source(var('benchmark'), 'ProspectRawuno') }}

union all

select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'ProspectRawdos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'ProspectRawtres') }}
