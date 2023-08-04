{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

SELECT
    [value],
    SUBSTRING([value], 16, 3) AS rectype
FROM
    {{ source('tpcdi', 'FinWireStg') }}
