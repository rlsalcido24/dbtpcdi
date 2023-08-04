{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

select *, substring(value, 16, 3) rectype from 
{{ source('tpcdi', 'FinWireStg') }}