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
    a.taxstatus,
    a.status,
    a.batchid,
    a.effectivedate,
    CONCAT(CONCAT(a.accountid, '-'), a.effectivedate) AS sk_accountid,
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
        --if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,  -- noqa: LT05
        CASE
            WHEN a.effectivedate < c.effectivedate THEN c.effectivedate ELSE
                a.effectivedate
        END AS effectivedate,
        --if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
        CASE
            WHEN a.enddate > c.enddate THEN c.enddate ELSE a.enddate
        END AS enddate
    FROM (
        SELECT *
        FROM (
            SELECT
                accountid,
                customerid,
                --coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (  -- noqa: LT05
                COALESCE(accountdesc, LAST_VALUE(accountdesc) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS accountdesc,
                --coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
                COALESCE(taxstatus, LAST_VALUE(taxstatus) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS taxstatus,
                --coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
                COALESCE(brokerid, LAST_VALUE(brokerid) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS brokerid,
                --coalesce(status, last_value(status) IGNORE NULLS OVER (
                COALESCE(status, LAST_VALUE(status) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS status,
                --date(update_ts) effectivedate,
                TO_DATE(update_ts, 'YYYYMMDD') AS effectivedate,
                --nvl(lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), date('9999-12-31')) enddate,  -- noqa: LT05
                ISNULL(
                    LEAD(TO_DATE(update_ts, 'YYYYMMDD'))
                        OVER (PARTITION BY accountid ORDER BY update_ts),
                    TO_DATE('9999-12-31', 'YYYY-MM-DD')
                ) AS enddate,
                batchid
            FROM (
                SELECT
                    CAST(accountid AS INT) AS accountid,
                    CAST(customerid AS INT) AS customerid,
                    accountdesc,
                    CAST(taxstatus AS INT) AS taxstatus,
                    CAST(brokerid AS INT) AS brokerid,
                    status,
                    TO_TIMESTAMP(
                        update_ts, 'YYYY-MM-DD HH24:MI:SS'
                    ) AS update_ts,
                    1 AS batchid
                FROM {{ ref('customermgmtview') }} AS c
                --FROM stg.CustomerMgmt c
                WHERE
                    actiontype NOT IN ('UPDCUST', 'INACT')
                    AND (TRIM(accountid) = '') IS NOT FALSE
                    AND (TRIM(customerid) = '') IS NOT FALSE
                    AND (TRIM(taxstatus) = '') IS NOT FALSE
                    AND (TRIM(brokerid) = '') IS NOT FALSE
                UNION ALL
                SELECT
                    accountid,
                    a.ca_c_id AS customerid,
                    accountdesc,
                    taxstatus,
                    a.ca_b_id AS brokerid,
                    st.st_name AS status,
                    --IMESTAMP(bd.batchdate) update_ts,
                    TO_TIMESTAMP(
                        bd.batchdate, 'YYYY-MM-DD HH24:MI:SS'
                    ) AS batchdate,
                    --convert(datetime2, bd.batchdate) update_ts,
                    a.batchid
                FROM {{ ref('accountincremental') }} AS a
                    --FROM stg.AccountIncremental a
                    INNER JOIN {{ ref('batchdate') }} AS bd
                        --JOIN dbo.BatchDate bd
                        ON a.batchid = bd.batchid
                    INNER JOIN {{ source('tpcdi', 'StatusType') }} AS st
                        --JOIN sf10.StatusType st
                        ON a.ca_st_id = st.st_id
            ) AS a
        ) AS a
        WHERE a.effectivedate < a.enddate
    ) AS a
        FULL OUTER JOIN {{ ref('dimcustomerstg') }} AS c
            --FULL OUTER JOIN dbo.DimCustomerStg c
            ON
                a.customerid = c.customerid
                AND c.enddate > a.effectivedate
                AND c.effectivedate < a.enddate
) AS a
    LEFT JOIN {{ ref('dimbroker') }} AS b
        --LEFT JOIN dbo.DimBroker b
        ON a.brokerid = b.brokerid
