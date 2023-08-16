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
    CONCAT(CONCAT(a.accountid, '-'), a.effectivedate) AS sk_accountid,
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
        CASE
            WHEN a.enddate > c.enddate THEN c.enddate ELSE a.enddate
        END AS enddate
    FROM (
        SELECT *
        FROM (
            SELECT
                accountid,
                customerid,
                COALESCE(accountdesc, LAST_VALUE(accountdesc) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS accountdesc,
                COALESCE(taxstatus, LAST_VALUE(taxstatus) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS taxstatus,
                COALESCE(brokerid, LAST_VALUE(brokerid) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS brokerid,
                COALESCE(status, LAST_VALUE(status) OVER (
                    PARTITION BY accountid
                    ORDER BY
                        update_ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS status,
                --date(update_ts) effectivedate,
                TO_DATE(update_ts, 'YYYYMMDD') AS effectivedate,
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
                FROM {{ ref('customermgmtview') }}
                WHERE
                    actiontype NOT IN ('UPDCUST', 'INACT')
                    AND (TRIM(accountid) = '') IS NOT FALSE
                    AND (TRIM(customerid) = '') IS NOT FALSE
                    AND (TRIM(taxstatus) = '') IS NOT FALSE
                    AND (TRIM(brokerid) = '') IS NOT FALSE
                UNION ALL
                SELECT
                    a.accountid,
                    a.ca_c_id AS customerid,
                    a.accountdesc,
                    a.taxstatus,
                    a.ca_b_id AS brokerid,
                    st.st_name AS status,
                    TO_TIMESTAMP(
                        bd.batchdate, 'YYYY-MM-DD HH24:MI:SS'
                    ) AS batchdate,
                    a.batchid
                FROM {{ ref('accountincremental') }} AS a
                    INNER JOIN {{ ref('batchdate') }} AS bd
                        ON a.batchid = bd.batchid
                    INNER JOIN {{ source('tpcdi', 'StatusType') }} AS st
                        ON a.ca_st_id = st.st_id
            ) AS a
        ) AS a
        WHERE a.effectivedate < a.enddate
    ) AS a
        FULL OUTER JOIN {{ ref('dimcustomerstg') }} AS c
            ON
                a.customerid = c.customerid
                AND c.enddate > a.effectivedate
                AND c.effectivedate < a.enddate
) AS a
    LEFT JOIN {{ ref('dimbroker') }} AS b
        ON a.brokerid = b.brokerid
