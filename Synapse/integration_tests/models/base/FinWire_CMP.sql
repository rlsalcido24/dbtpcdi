{{
    config(
        materialized = 'table'
    )
}}

SELECT *
FROM
    {{ ref('FinWire') }}
WHERE rectype = 'CMP'
