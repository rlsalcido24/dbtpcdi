{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'ProspectRaw1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'ProspectRaw2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'ProspectRaw3') }}