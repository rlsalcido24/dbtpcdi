{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )
}}


select
    *
from
    {{ source('tpcdi', 'CustomerMgmt') }}
