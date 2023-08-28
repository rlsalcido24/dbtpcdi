{{
    config(
        materialized = 'table'
    )
}}

SELECT *
FROM
    {{ ref('FinWire') }}
WHERE rectype = 'SEC'
