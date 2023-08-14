{{
    config(
        materialized = 'table'
    )
}}
SELECT
    c.sk_customerid AS sk_customerid,
    s.sk_securityid AS sk_securityid,
    wh.sk_dateid_dateplaced,
    wh.sk_dateid_dateremoved,
    wh.batchid
FROM (
    SELECT * EXCEPT (w_dts)
    FROM (
        SELECT -- noqa: ST06
            customerid,
            symbol,
            COALESCE(
                sk_dateid_dateplaced,
                LAST_VALUE(sk_dateid_dateplaced IGNORE NULLS)
                    OVER (
                        PARTITION BY customerid, symbol
                        ORDER BY
                            w_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS sk_dateid_dateplaced,
            COALESCE(
                sk_dateid_dateremoved,
                LAST_VALUE(sk_dateid_dateremoved IGNORE NULLS)
                    OVER (
                        PARTITION BY customerid, symbol
                        ORDER BY
                            w_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS sk_dateid_dateremoved,
            COALESCE(
                dateplaced,
                LAST_VALUE(dateplaced IGNORE NULLS)
                    OVER (
                        PARTITION BY customerid, symbol
                        ORDER BY
                            w_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS dateplaced,
            w_dts,
            COALESCE(
                batchid,
                LAST_VALUE(batchid IGNORE NULLS)
                    OVER (
                        PARTITION BY customerid, symbol
                        ORDER BY
                            w_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS batchid
        FROM (
            SELECT  -- noqa: ST06
                wh.w_c_id AS customerid,
                wh.w_s_symb AS symbol,
                IF(
                    wh.w_action = 'ACTV',
                    d.sk_dateid,
                    NULL
                ) AS sk_dateid_dateplaced,
                IF(
                    wh.w_action = 'CNCL',
                    d.sk_dateid,
                    NULL
                ) AS sk_dateid_dateremoved,
                IF(
                    wh.w_action = 'ACTV',
                    d.datevalue,
                    NULL
                ) AS dateplaced,
                wh.w_dts,
                wh.batchid
            FROM (
                SELECT
                    *,
                    1 AS batchid
                FROM
                    {{ source(var('benchmark'),'WatchHistory') }}
                UNION ALL
                SELECT
                    * EXCEPT (
                        cdc_flag,
                        cdc_dsn
                    )
                FROM
                    {{ ref('WatchIncremental') }}
            ) AS wh
                INNER JOIN
                    {{ source(var('benchmark'),'DimDate') }} AS d
                    ON
                        d.datevalue = DATE(wh.w_dts)
        )
    ) QUALIFY
        ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts DESC)
        = 1
) AS wh
-- Converts to LEFT JOINs if this is run as DQ EDITION. On some higher Scale
-- Factors, a small number of Security symbols or Customer IDs "may" be missing
-- from DimSecurity/DimCustomer, causing audit check failures.
--${dq_left_flg}
    LEFT JOIN
        {{ ref('DimSecurity') }} AS s
        ON
            s.symbol = wh.symbol
            AND wh.dateplaced >= s.effectivedate
            AND wh.dateplaced < s.enddate
    --${dq_left_flg}
    LEFT JOIN
        {{ ref('DimCustomer') }} AS c
        ON
            wh.customerid = c.customerid
            AND wh.dateplaced >= c.effectivedate
            AND wh.dateplaced < c.enddate
