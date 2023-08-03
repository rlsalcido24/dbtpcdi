{{
    config(
        materialized = 'view',
        partition_by = 'rectype', bind=False
    )
}}

select
    [value],
    SUBSTRING([value],16,3) as rectype
from
    {{ source('tpcdi', 'FinWireStg') }}
