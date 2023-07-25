{{
    config(
        materialized = 'view', bind=False
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'AccountIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'AccountIncremental3') }}
