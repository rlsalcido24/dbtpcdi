{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(dm_s_symb)'
    )
}}


SELECT
  dmh.*,
  sk_dateid,
  min(dm_low) OVER (
    PARTITION BY dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweeklow,
  max(dm_high) OVER (
    PARTITION by dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweekhigh
FROM (
  SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol FROM {{ ref('DailyMarketHistorical') }}
  UNION ALL
  SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol FROM {{ ref('DailyMarketIncremental') }}
) dmh
JOIN {{ ref('DimDate') }} d 
  ON d.datevalue = dm_date;