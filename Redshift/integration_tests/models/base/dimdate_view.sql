{{
    config(
        materialized = 'view', bind=False
    )
}}
SELECT *
FROM
    {{ source('tpcdi', 'DimDate') }}
