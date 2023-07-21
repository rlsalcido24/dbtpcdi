{{
    config(
        materialized = 'view', bind=False
    )
}}
select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'DailyMarketHistorical') }}