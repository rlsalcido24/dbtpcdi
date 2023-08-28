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
        OVER (
            PARTITION BY c.accountid
            ORDER BY
                c.datevalue
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    AS cash,
    c.batchid
FROM (
    SELECT
        ct_ca_id AS accountid,
        TO_DATE(ct_dts, 'YYYYMMDD') AS datevalue,
        SUM(ct_amt) AS account_daily_total,
        batchid
    FROM (
        SELECT
            *,
            1 AS batchid
        FROM {{ source('tpcdi', 'CashTransactionHistory') }}
        UNION ALL
        SELECT
            ct_ca_id,
            ct_dts,
            ct_amt,
            ct_name,
            batchid
        FROM {{ ref('cashtransactionincremental') }}
    ) AS t
    GROUP BY
        ct_ca_id,
        TO_DATE(ct_dts, 'YYYYMMDD'),
        batchid
) AS c
    INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
        ON c.datevalue = d.datevalue
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Account IDs are missing from dimaccount,
    -- causing audit check failures.
    LEFT JOIN {{ ref( 'dimaccount') }} AS a
        ON
            c.accountid = a.accountid
            AND c.datevalue >= a.effectivedate
            AND c.datevalue < a.enddate
