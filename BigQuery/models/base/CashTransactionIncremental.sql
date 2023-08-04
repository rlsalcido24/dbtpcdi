{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source(var('benchmark'), 'CashTransactionIncrementaldos') }}

union all

select
    *,
    3 as batchid
from
    {{ source(var('benchmark'), 'CashTransactionIncrementaltres') }}
