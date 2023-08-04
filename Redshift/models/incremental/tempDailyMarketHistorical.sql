{{
    config(
        materialized = 'table'

    )
}}
SELECT
    dmh.*,
    sk_dateid,
    MIN(dm_low) OVER (
        PARTITION BY dm_s_symb
        ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweeklow,
    MAX(dm_high) OVER (
        PARTITION BY dm_s_symb
        ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweekhigh
FROM (
    SELECT
        CAST(dm_date AS DATE) dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('dailymarkethistorical') }}
    UNION ALL
    SELECT
        CAST(dm_date AS DATE) dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('dailymarketincremental') }}
) dmh
    JOIN {{ source('tpcdi', 'DimDate') }} d
        --JOIN prd.DimDate d
        ON d.datevalue = dm_date
