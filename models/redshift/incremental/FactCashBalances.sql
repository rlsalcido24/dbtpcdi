{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='HASH(sk_customerid)'

SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  d.sk_dateid, 
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cash,
  c.batchid
FROM (
  SELECT 
    ct_ca_id accountid,
    --to_date(ct_dts) datevalue,
    to_date(ct_dts,'YYYYMMDD') datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM (
    SELECT * , 1 batchid
    FROM {{ source('tpcdi', 'CashTransactionHistory') }}
    --FROM prd.CashTransactionHistory
    UNION ALL
    --SELECT * except(cdc_flag, cdc_dsn)
    SELECT ct_ca_id, ct_dts, ct_amt, ct_name, batchid
    FROM {{ ref('cashtransactionincremental') }}
    --FROM stg.CashTransactionIncremental
  ) T
  GROUP BY
    --accountid,
    ct_ca_id,
    --datevalue,
    to_date(ct_dts,'YYYYMMDD'),
    batchid
) c 
JOIN {{ source('tpcdi', 'DimDate') }} d 
--JOIN prd.DimDate d 
  ON c.datevalue = d.datevalue
-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Account IDs are missing from dimaccount, causing audit check failures. 
 LEFT JOIN {{ ref( 'dimaccount') }} a 
-- LEFT JOIN dbo.dimaccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate 