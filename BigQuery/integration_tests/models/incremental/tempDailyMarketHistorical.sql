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
    SELECT * FROM {{ ref('DailyMarketHistorical') }}
    UNION ALL
    SELECT * EXCEPT (cdc_flag, cdc_dsn) FROM {{ ref('DailyMarketIncremental') }}
) dmh
    JOIN {{ source(var('benchmark'), 'DimDate') }} d
        ON d.datevalue = dm_date
