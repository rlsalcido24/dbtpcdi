{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(dm_s_symb)'
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
        dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('DailyMarketHistorical') }}
    UNION ALL
    SELECT
        dm_date,
        dm_s_symb,
        dm_close,
        dm_high,
        dm_low,
        dm_vol
    FROM {{ ref('DailyMarketIncremental') }}
) AS dmh
    INNER JOIN {{ ref('DimDate') }} AS d
        ON d.datevalue = dmh.dm_date;
