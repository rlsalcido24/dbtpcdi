{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--, dist='HASH(sk_customerid)'



SELECT
    c.sk_customerid AS sk_customerid,
    s.sk_securityid AS sk_securityid,
    wh.sk_dateid_dateplaced,
    wh.sk_dateid_dateremoved,
    wh.batchid
FROM
    (
        SELECT *
        FROM
            (
                SELECT
                    --* EXCEPT(w_dts)
                    customerid,
                    symbol,
                    sk_dateid_dateplaced,
                    sk_dateid_dateremoved,
                    dateplaced,
                    batchid,
                    ROW_NUMBER()
                        OVER (
                            PARTITION BY customerid, symbol ORDER BY w_dts DESC
                        )
                    AS rownum
                FROM
                    (
                        SELECT
                            customerid,
                            symbol,
                            --coalesce(sk_dateid_dateplaced, last_value(sk_dateid_dateplaced) IGNORE NULLS OVER ( -- noqa: LT05
                            COALESCE(
                                sk_dateid_dateplaced,
                                LAST_VALUE(sk_dateid_dateplaced) OVER (
                                    PARTITION BY customerid, symbol
                                    ORDER BY
                                        w_dts
                                    ROWS BETWEEN UNBOUNDED PRECEDING
                                    AND CURRENT ROW
                                )
                            ) AS sk_dateid_dateplaced,
                            --coalesce(sk_dateid_dateremoved, last_value(sk_dateid_dateremoved) IGNORE NULLS OVER ( -- noqa: LT05
                            COALESCE(
                                sk_dateid_dateremoved,
                                LAST_VALUE(sk_dateid_dateremoved) OVER (
                                    PARTITION BY customerid, symbol
                                    ORDER BY
                                        w_dts
                                    ROWS BETWEEN UNBOUNDED PRECEDING
                                    AND CURRENT ROW
                                )
                            ) AS sk_dateid_dateremoved,
                            --coalesce(dateplaced, last_value(dateplaced) IGNORE NULLS OVER ( -- noqa: LT05
                            COALESCE(dateplaced, LAST_VALUE(dateplaced) OVER (
                                PARTITION BY customerid, symbol
                                ORDER BY
                                    w_dts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )) AS dateplaced,
                            w_dts,
                            --coalesce(batchid, last_value(batchid) IGNORE NULLS OVER ( -- noqa: LT05
                            COALESCE(batchid, LAST_VALUE(batchid) OVER (
                                PARTITION BY customerid, symbol
                                ORDER BY
                                    t0.w_dts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )) AS batchid
                        FROM
                            (
                                SELECT
                                    wh.w_c_id AS customerid,
                                    wh.w_s_symb AS symbol,
                                    -- if(w_action = 'ACTV', d.sk_dateid, null) sk_dateid_dateplaced, -- noqa: LT05
                                    CASE
                                        WHEN
                                            wh.w_action = 'ACTV'
                                            THEN d.sk_dateid
                                    END AS sk_dateid_dateplaced,
                                    -- if(w_action = 'CNCL', d.sk_dateid, null) sk_dateid_dateremoved, -- noqa: LT05
                                    CASE
                                        WHEN
                                            wh.w_action = 'CNCL'
                                            THEN d.sk_dateid
                                    END AS sk_dateid_dateremoved,
                                    --if(w_action = 'ACTV', d.datevalue, null) dateplaced, -- noqa: LT05
                                    CASE
                                        WHEN
                                            wh.w_action = 'ACTV'
                                            THEN d.datevalue
                                    END AS dateplaced,
                                    wh.w_dts,
                                    wh.batchid
                                FROM
                                    (
                                        SELECT
                                            *,
                                            1 AS batchid
                                        FROM
                                            {{ source('tpcdi', 'WatchHistory') }} -- noqa: LT05
                                        --FROM prd.WatchHistory
                                        UNION ALL
                                        SELECT
                                            --    * except(cdc_flag, cdc_dsn)
                                            w_c_id,
                                            w_s_symb,
                                            w_dts,
                                            w_action,
                                            batchid
                                        FROM {{ ref('watchincremental') }}
                                    --FROM stg.watchincremental
                                    ) AS wh
                                    INNER JOIN
                                        {{ source('tpcdi', 'DimDate') }} AS d
                                        ON
                                            d.datevalue
                                            = TO_DATE(wh.w_dts, 'YYYYMMDD')
                            --JOIN prd.DimDate d ON d.datevalue = convert(date,wh.w_dts) -- noqa: LT05
                            ) AS t0
                    ) AS t1
            ) AS t
        WHERE t.rownum = 1
    ) AS wh
    --  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) = 1) wh -- noqa: LT05
    -- Converts to LEFT JOINs if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Security symbols or Customer IDs "may" be
    -- missing from DimSecurity/DimCustomer, causing audit check failures.
    --${dq_left_flg}
    LEFT JOIN {{ ref('dimsecurity') }} AS s
        --LEFT JOIN dbo.DimSecurity s
        ON
            s.symbol = wh.symbol
            AND wh.dateplaced >= s.effectivedate
            AND wh.dateplaced < s.enddate
    --${dq_left_flg}
    LEFT JOIN {{ ref('dimcustomer') }} AS c
        --LEFT JOIN dbo.DimCustomer c
        ON
            wh.customerid = c.customerid
            AND wh.dateplaced >= c.effectivedate
            AND wh.dateplaced < c.enddate
