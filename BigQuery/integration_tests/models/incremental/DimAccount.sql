{{
    config(
        materialized = 'table'
    )
}}
SELECT
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.batchid,
  a.effectivedate,
  CONCAT(a.accountid, '-', a.effectivedate) AS sk_accountid,
  a.enddate
FROM (
  SELECT
    a.* EXCEPT (effectivedate, enddate, customerid),
    c.sk_customerid,
    IF(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) AS effectivedate,
    IF(a.enddate > c.enddate, c.enddate, a.enddate) AS enddate
  FROM (
    SELECT *
    FROM (
      SELECT
        accountid,
        customerid,
        COALESCE(accountdesc, LAST_VALUE(accountdesc IGNORE NULLS) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS accountdesc,
        COALESCE(taxstatus, LAST_VALUE(taxstatus IGNORE NULLS) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS taxstatus,
        COALESCE(brokerid, LAST_VALUE(brokerid IGNORE NULLS) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS brokerid,
        COALESCE(status, LAST_VALUE(status IGNORE NULLS) OVER (
          PARTITION BY accountid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS status,
        DATE(update_ts) AS effectivedate,
        IFNULL(LEAD(DATE(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), DATE('9999-12-31')) AS enddate,
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
          1 AS batchid
        FROM {{ ref('CustomerMgmtView') }} c
        WHERE ActionType NOT IN ('UPDCUST', 'INACT')
        UNION ALL
        SELECT
          accountid,
          a.ca_c_id AS customerid,
          accountDesc,
          TaxStatus,
          a.ca_b_id AS brokerid,
          st_name AS status,
          TIMESTAMP(bd.batchdate) AS update_ts,
          a.batchid
        FROM {{ ref('AccountIncremental') }} a
        JOIN {{ ref('BatchDate') }} bd ON a.batchid = bd.batchid
        JOIN {{source(var('benchmark'),'StatusType') }} st ON a.CA_ST_ID = st.st_id
      ) a
    ) a
    WHERE a.effectivedate < a.enddate
  ) a
  FULL OUTER JOIN {{ ref('DimCustomerStg') }} c
    ON a.customerid = c.customerid
    AND c.enddate > a.effectivedate
    AND c.effectivedate < a.enddate
) a
LEFT JOIN {{ ref('DimBroker') }} b
  ON a.brokerid = b.brokerid