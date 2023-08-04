{{
    config(
        materialized = 'view', bind=False
    )
}}

select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'BatchDate1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'BatchDate2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'BatchDate3') }}
