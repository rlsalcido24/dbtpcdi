{{
    config(
        materialized = 'table'
    )
}}

SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  d.sk_dateid, 
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash,
  c.batchid
FROM (
  SELECT 
    ct_ca_id accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM (
    SELECT * , 1 batchid
    FROM LIVE.CashTransactionHistory
    UNION ALL
    SELECT * except(cdc_flag, cdc_dsn)
    FROM LIVE.CashTransactionIncremental
  )
  GROUP BY
    accountid,
    datevalue,
    batchid) c 
JOIN LIVE.DimDate d 
  ON c.datevalue = d.datevalue
-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Account IDs are missing from DimAccount, causing audit check failures. 
${dq_left_flg} JOIN LIVE.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate 
