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
    SELECT * FROM {{ ref('DailyMarketHistorical') }}
    UNION ALL
    SELECT * EXCEPT (cdc_flag, cdc_dsn) FROM {{ ref('DailyMarketIncremental') }}
) AS dmh
    INNER JOIN {{ source(var('benchmark'), 'DimDate') }} AS d
        ON d.datevalue = dmh.dm_date
