{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

SELECT
    *,
    SUBSTRING(value, 16, 3) rectype
FROM
    {{ source('tpcdi', 'FinWireStg') }}
