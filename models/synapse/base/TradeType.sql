{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}


select
    *
from
    {{ source('tpcdi', 'TradeType') }}
