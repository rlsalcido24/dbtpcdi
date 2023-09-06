{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(accountid)'
    )
}}

-- !!!!! IGNORE NULLS is not supported in Synapse !!!!!

SELECT
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  a.batchid,
  a.effectivedate,
  concat(a.accountid, '-', a.effectivedate) as sk_accountid,
  a.enddate
FROM (
  SELECT
    a.accountid,
    a.accountdesc,
    a.taxstatus,
    a.brokerid,
    a.status,
    a.batchid,
    c.sk_customerid,
    CASE WHEN a.effectivedate < c.effectivedate THEN c.effectivedate ELSE a.effectivedate END effectivedate,
    CASE WHEN a.enddate > c.enddate THEN c.enddate ELSE a.enddate END enddate
  FROM (
    SELECT *
    FROM (
      SELECT
        accountid,
        customerid,
        --coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
        coalesce(accountdesc, last_value(accountdesc) OVER (
          PARTITION BY accountid ORDER BY update_ts)) accountdesc,
        --coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
        coalesce(taxstatus, last_value(taxstatus) OVER (
          PARTITION BY accountid ORDER BY update_ts)) taxstatus,
        --coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
        coalesce(brokerid, last_value(brokerid) OVER (
          PARTITION BY accountid ORDER BY update_ts)) brokerid,
        --coalesce(status, last_value(status) IGNORE NULLS OVER (
        coalesce(status, last_value(status) OVER (
          PARTITION BY accountid ORDER BY update_ts)) status,
        convert(date,update_ts) effectivedate,
        isnull(lead(convert(date,update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), convert(date,'9999-12-31')) enddate,
        batchid
      FROM (
        SELECT
          accountid,
          customerid,
          accountdesc,
          taxstatus,
          brokerid,
          status,
          update_ts,
          1 batchid
        FROM {{ ref('CustomerMgmt') }}  c
        WHERE ActionType NOT IN ('UPDCUST', 'INACT')
        UNION ALL
        SELECT
          accountid,
          a.ca_c_id customerid,
          accountDesc,
          taxstatus,
          a.ca_b_id brokerid,
          st_name as status,
          convert(datetime2, bd.batchdate) update_ts,
          a.batchid
        FROM {{ ref('AccountIncremental') }} a
        JOIN {{ ref('BatchDate') }} bd
          ON a.batchid = bd.batchid
        JOIN {{ ref('StatusType') }} st 
          ON a.ca_st_id = st.st_id
      ) a
    ) a
    WHERE a.effectivedate < a.enddate
  ) a
  FULL OUTER JOIN {{ ref('DimCustomerStg') }} c 
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
) a
LEFT JOIN {{ ref('DimBroker') }} b 
  ON a.brokerid = b.brokerid;