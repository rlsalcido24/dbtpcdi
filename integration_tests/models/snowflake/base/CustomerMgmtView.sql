{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    {{ source('tpcdi', 'CustomerMgmt') }}

