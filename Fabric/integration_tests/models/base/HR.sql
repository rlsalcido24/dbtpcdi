{{
    config(
        materialized = 'view'
    )
}}


select
    *
from
    {{ source('tpcdi', 'HR') }}
