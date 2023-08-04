{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='HASH(sk_companyid)'

SELECT 
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  hh.batchid
FROM (
  SELECT 
    * ,
    1 batchid
  FROM {{ source('tpcdi', 'HoldingHistory') }}
  --FROM prd.holdinghistory
  UNION ALL
  --SELECT * except(cdc_flag, cdc_dsn)
  SELECT 
    [hh_h_t_id]
    ,[hh_t_id]
    ,[hh_before_qty]
    ,[hh_after_qty]
    ,[BATCHID]
  FROM {{ ref('holdingincremental') }}) hh
  --FROM stg.HoldingIncremental) hh
-- Converts to LEFT JOIN if this is run as DQ EDITION. It is possible, because of the issues upstream with DimSecurity/DimAccount on "some" scale factors, that dimtrade may be missing some rows.
--${dq_left_flg}
 LEFT JOIN {{ ref('dimtrade') }} dt
 --LEFT JOIN dbo.dimtrade dt
  ON tradeid = hh_t_id