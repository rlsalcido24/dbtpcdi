{{
    config(
        materialized = 'table'
    )
}}

select
    *
from
    {{ ref('FinWire') }}
where rectype='INT'
