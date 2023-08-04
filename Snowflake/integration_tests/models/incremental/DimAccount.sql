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
    CONCAT(a.accountid, '-', a.effectivedate) AS sk_accountid,
    a.enddate
FROM (
    SELECT
        a.* EXCLUDE (effectivedate, enddate, customerid),
        c.sk_customerid,
        IFF(
            a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate
        ) effectivedate,
        IFF(a.enddate > c.enddate, c.enddate, a.enddate) enddate
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
                ) accountdesc,
                COALESCE(taxstatus, LAST_VALUE(taxstatus) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) taxstatus,
                COALESCE(brokerid, LAST_VALUE(brokerid) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) brokerid,
                COALESCE(status, LAST_VALUE(status) IGNORE NULLS OVER (
                    PARTITION BY accountid ORDER BY update_ts
                )) status,
                DATE(update_ts) effectivedate,
                COALESCE(
                    LEAD(DATE(update_ts))
                        OVER (PARTITION BY accountid ORDER BY update_ts),
                    DATE('9999-12-31')
                ) enddate,
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
                FROM {{ ref('CustomerMgmtView') }} c
                WHERE actiontype NOT IN ('UPDCUST', 'INACT')
                UNION ALL
                SELECT
                    accountid,
                    a.ca_c_id customerid,
                    accountdesc,
                    taxstatus,
                    a.ca_b_id brokerid,
                    st_name AS status,
                    TO_TIMESTAMP(bd.batchdate) update_ts,
                    a.batchid
                FROM {{ ref('AccountIncremental') }} a
                    JOIN {{ ref('BatchDate') }} bd
                        ON a.batchid = bd.batchid
                    JOIN {{ source('tpcdi', 'StatusType') }} st
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
        ON a.brokerid = b.brokerid
