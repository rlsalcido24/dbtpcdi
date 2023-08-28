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
    a.taxstatus,
    a.status,
    a.batchid,
    a.effectivedate,
    BIGINT(
        CONCAT(
            DATE_FORMAT(a.effectivedate, 'yyyyMMdd'),
            CAST(a.accountid AS STRING)
        )
    ) AS sk_accountid,
    a.enddate
FROM (
    SELECT
        a.* EXCEPT (effectivedate, enddate, customerid),
        c.sk_customerid,
        IF(
            a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate
        ) AS effectivedate,
        IF(a.enddate > c.enddate, c.enddate, a.enddate) AS enddate
    FROM (
        SELECT *
        FROM (
            SELECT
                accountid,
                customerid,
                COALESCE(
                    accountdesc, LAST_VALUE(accountdesc) IGNORE NULLS OVER (
                        PARTITION BY accountid ORDER BY update_ts
                    )
                ) AS accountdesc,
                COALESCE(taxstatus, LAST_VALUE(taxstatus) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS taxstatus,
                COALESCE(brokerid, LAST_VALUE(brokerid) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS brokerid,
                COALESCE(status, LAST_VALUE(status) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) AS status,
                DATE(update_ts) AS effectivedate,
                COALESCE(
                    LEAD(DATE(update_ts))
                        OVER (PARTITION BY accountid ORDER BY update_ts),
                    DATE('9999-12-31')
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
                FROM {{ ref('CustomerMgmtView') }}
                WHERE actiontype NOT IN ('UPDCUST', 'INACT')
                UNION ALL
                SELECT
                    a.accountid,
                    a.ca_c_id AS customerid,
                    a.accountdesc,
                    a.taxstatus,
                    a.ca_b_id AS brokerid,
                    st.st_name AS status,
                    TIMESTAMP(bd.batchdate) AS update_ts,
                    a.batchid
                FROM {{ ref('AccountIncremental') }} AS a
                    INNER JOIN {{ ref('BatchDate') }} AS bd
                        ON a.batchid = bd.batchid
                    INNER JOIN {{ source('tpcdi', 'StatusType') }} AS st
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
