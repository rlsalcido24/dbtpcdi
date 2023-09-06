{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

select
    [value],
    SUBSTRING([value],16,3) as rectype
from
    {{ source('tpcdi', 'FinWireStg') }}
