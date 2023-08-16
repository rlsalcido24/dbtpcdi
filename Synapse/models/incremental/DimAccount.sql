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
    CONCAT(a.accountid, '-', a.effectivedate) AS sk_accountid,
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
        CASE
            WHEN a.effectivedate < c.effectivedate THEN c.effectivedate ELSE
                a.effectivedate
        END AS effectivedate,
        CASE WHEN a.enddate > c.enddate THEN c.enddate ELSE a.enddate END
            AS enddate
    FROM (
        SELECT *
        FROM (
            SELECT
                accountid,
                customerid,
                --coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER ( -- noqa: LT05
                COALESCE(accountdesc, LAST_VALUE(accountdesc) OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS accountdesc,
                --coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
                COALESCE(taxstatus, LAST_VALUE(taxstatus) OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS taxstatus,
                --coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
                COALESCE(brokerid, LAST_VALUE(brokerid) OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS brokerid,
                --coalesce(status, last_value(status) IGNORE NULLS OVER (
                COALESCE(status, LAST_VALUE(status) OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS status,
                CAST(update_ts AS DATE) AS effectivedate,
                ISNULL(
                    LEAD(CAST(update_ts AS DATE))
                        OVER (PARTITION BY accountid ORDER BY update_ts),
                    CAST('9999-12-31' AS DATE)
                ) AS enddate,
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
                FROM {{ ref('CustomerMgmt') }}
                WHERE actiontype NOT IN ('UPDCUST', 'INACT')
                UNION ALL
                SELECT
                    a.accountid,
                    a.ca_c_id AS customerid,
                    a.accountdesc,
                    a.taxstatus,
                    a.ca_b_id AS brokerid,
                    st.st_name AS status,
                    CAST(bd.batchdate AS DATETIME2) AS update_ts,
                    a.batchid
                FROM {{ ref('AccountIncremental') }} AS a
                    INNER JOIN {{ ref('BatchDate') }} AS bd
                        ON a.batchid = bd.batchid
                    INNER JOIN {{ ref('StatusType') }} AS st
                        ON a.ca_st_id = st.st_id
            ) AS a
        ) AS a
        WHERE effectivedate < enddate
    ) AS a
        FULL OUTER JOIN {{ ref('DimCustomerStg') }} AS c
            ON
                a.customerid = c.customerid
                AND c.enddate > a.effectivedate
                AND c.effectivedate < a.enddate
) AS a
    LEFT JOIN {{ ref('DimBroker') }} AS b
        ON a.brokerid = b.brokerid;
