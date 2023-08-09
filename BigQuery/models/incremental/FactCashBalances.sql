{{
    config(
        materialized = 'table'
    )
}}

SELECT
    a.sk_customerid,
    a.sk_accountid,
    d.sk_dateid,
    SUM(c.account_daily_total)
        OVER (PARTITION BY c.accountid ORDER BY c.datevalue) AS cash,
    c.batchid
FROM (
    SELECT
        ct_ca_id AS accountid,
        DATE(ct_dts) AS datevalue,
        SUM(ct_amt) AS account_daily_total,
        batchid
    FROM (
        SELECT
            *,
            1 AS batchid
        FROM {{ source(var('benchmark'), 'CashTransactionHistory') }}
        UNION ALL
        SELECT * EXCEPT (cdc_flag, cdc_dsn)
        FROM {{ ref('CashTransactionIncremental') }}
    )
    GROUP BY
        accountid,
        datevalue,
        batchid
) AS c
    INNER JOIN {{ source(var('benchmark'), 'DimDate') }} AS d
        ON c.datevalue = d.datevalue
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Account IDs are missing from DimAccount,
    -- causing audit check failures.
    LEFT JOIN {{ ref( 'DimAccount') }} AS a
        ON
            c.accountid = a.accountid
            AND c.datevalue >= a.effectivedate
            AND c.datevalue < a.enddate
