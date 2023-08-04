{{
    config(
        materialized = 'view', bind=False
    )
}}
select
    *
from
    {{ source('tpcdi', 'HR') }}
