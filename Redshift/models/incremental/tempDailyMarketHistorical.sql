{{
    config(
        materialized = 'table'

    )
}}
SELECT
    dmh.*,
    d.sk_dateid,
    MIN(dmh.dm_low) OVER (
        PARTITION BY dmh.dm_s_symb
        ORDER BY dmh.dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    MAX(dmh.dm_high) OVER (
        PARTITION BY dmh.dm_s_symb
        ORDER BY dmh.dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
FROM (
    SELECT
        CAST(dm_date AS DATE) AS dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('dailymarkethistorical') }}
    UNION ALL
    SELECT
        CAST(dm_date AS DATE) AS dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('dailymarketincremental') }}
) AS dmh
    INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
        --JOIN prd.DimDate d
        ON d.datevalue = dmh.dm_date
