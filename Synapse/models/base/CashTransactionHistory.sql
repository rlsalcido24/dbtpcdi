{{
    config(
        materialized = 'view'
    )    
}}


SELECT *
FROM
    {{ source('tpcdi', 'CashTransactionHistory') }}
