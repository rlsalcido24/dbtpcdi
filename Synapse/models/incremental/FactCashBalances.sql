{{
    config(
        materialized = 'table'
    )
}}


SELECT
    a.sk_customerid,
    a.sk_accountid,
    d.sk_dateid,
    SUM(account_daily_total)
        OVER (PARTITION BY c.accountid ORDER BY c.datevalue)
        cash,
    c.batchid
FROM (
    SELECT
        ct_ca_id accountid,
        CONVERT(DATE, ct_dts) datevalue,
        SUM(ct_amt) account_daily_total,
        batchid
    FROM (
        SELECT
            *,
            1 batchid
        FROM {{ ref('CashTransactionHistory') }}
        UNION ALL
        SELECT
            ct_ca_id,
            ct_dts,
            ct_amt,
            ct_name,
            batchid
        FROM {{ ref('CashTransactionIncremental') }}
    ) t
    GROUP BY
    --accountid,
        ct_ca_id,
        --datevalue,
        CONVERT(DATE, ct_dts),
        batchid
) c
    JOIN {{ ref('DimDate') }} d
        ON c.datevalue = d.datevalue
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Account IDs are missing from DimAccount, causing audit check failures. 
    LEFT JOIN {{ ref( 'DimAccount') }} a
        ON
            c.accountid = a.accountid
            AND c.datevalue >= a.effectivedate
            AND c.datevalue < a.enddate
