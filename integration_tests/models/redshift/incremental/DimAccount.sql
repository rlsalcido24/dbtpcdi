{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='REPLICATE'



SELECT
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.batchid,
  a.effectivedate,
  concat(concat(a.accountid, '-'), a.effectivedate) as sk_accountid,
  a.enddate
FROM (
  SELECT
    --a.* except(effectivedate, enddate, customerid),
    a.accountid,
    a.accountdesc,
    a.taxstatus,
    a.brokerid,
    a.status,
    a.batchid,
    c.sk_customerid,
    --if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,
    CASE WHEN a.effectivedate < c.effectivedate THEN c.effectivedate ELSE a.effectivedate END effectivedate,
    --if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
    CASE WHEN a.enddate > c.enddate THEN c.enddate ELSE a.enddate END enddate
  FROM (
    SELECT *
    FROM (
      SELECT
        accountid,
        customerid,
        --coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
        coalesce(accountdesc, last_value(accountdesc) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) accountdesc,
        --coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
        coalesce(taxstatus, last_value(taxstatus) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) taxstatus,
        --coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
        coalesce(brokerid, last_value(brokerid) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) brokerid,
        --coalesce(status, last_value(status) IGNORE NULLS OVER (
        coalesce(status, last_value(status) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) status,
        --date(update_ts) effectivedate,
        TO_DATE(update_ts,'YYYYMMDD') effectivedate,
        --nvl(lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), date('9999-12-31')) enddate,
        isnull(lead(TO_DATE(update_ts,'YYYYMMDD')) OVER (PARTITION BY accountid ORDER BY update_ts), TO_DATE('9999-12-31','YYYY-MM-DD')) enddate,
        batchid
      FROM (
        SELECT
           cast(accountid as int) as accountid,
          cast(customerid as int) as customerid,
          accountdesc,
          cast(taxstatus as int) as taxstatus,
          cast(brokerid as int) as brokerid,
          status,
          to_timestamp(update_ts, 'YYYY-MM-DD HH24:MI:SS') update_ts,
          1 batchid
        FROM {{ ref('customermgmtview') }}  c
        --FROM stg.CustomerMgmt c
        WHERE ActionType NOT IN ('UPDCUST', 'INACT') AND (trim(accountid) = '') IS NOT FALSE AND (trim(customerid) = '') IS NOT FALSE
        AND (trim(taxstatus) = '') IS NOT FALSE AND (trim(brokerid) = '') IS NOT FALSE
        UNION ALL
        SELECT
          accountid,
          a.ca_c_id customerid,
          accountDesc,
          TaxStatus,
          a.ca_b_id brokerid,
          st_name as status,
          to_timestamp(bd.batchdate, 'YYYY-MM-DD HH24:MI:SS'),--IMESTAMP(bd.batchdate) update_ts,
          
          --convert(datetime2, bd.batchdate) update_ts,
          a.batchid
        FROM {{ ref('accountincremental') }} a
        --FROM stg.AccountIncremental a
        JOIN {{ ref('batchdate') }} bd
        --JOIN dbo.BatchDate bd
          ON a.batchid = bd.batchid
        JOIN {{ source('tpcdi', 'StatusType') }} st 
        --JOIN sf10.StatusType st 
          ON a.CA_ST_ID = st.st_id
      ) a
    ) a
    WHERE a.effectivedate < a.enddate
  ) a
  FULL OUTER JOIN {{ ref('dimcustomerstg') }} c 
  --FULL OUTER JOIN dbo.DimCustomerStg c 
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
) a
LEFT JOIN {{ ref('dimbroker') }} b 
--LEFT JOIN dbo.DimBroker b 
  ON a.brokerid = b.brokerid