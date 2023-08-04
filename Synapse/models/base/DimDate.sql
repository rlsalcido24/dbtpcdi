{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}



SELECT *
FROM
    {{ source('tpcdi', 'DimDate') }}
