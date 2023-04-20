
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
  SELECT * FROM `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`DailyMarketHistorical`
  UNION ALL
  SELECT * except(cdc_flag, cdc_dsn) FROM `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`DailyMarketIncremental`) dmh
JOIN `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`DimDate` d 
  ON d.datevalue = dm_date;