{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        , dist='HASH(sk_customerid)'
    )
}}

-- !!!!! IGNORE NULLS is not supported in Synapse !!!!!

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
                    customerid,
                    symbol,
                    sk_dateid_dateplaced,
                    sk_dateid_dateremoved,
                    dateplaced,
                    batchid,
                    ROW_NUMBER() OVER (
                        PARTITION BY customerid, symbol ORDER BY w_dts DESC
                    ) AS rownum
                FROM
                    (
                        SELECT
                            customerid,
                            symbol,
                            COALESCE(
                                sk_dateid_dateplaced,
                                LAST_VALUE(sk_dateid_dateplaced) /*IGNORE NULLS*/ OVER (
                                    PARTITION BY customerid,
                                    symbol ORDER BY w_dts
                                )
                            ) AS sk_dateid_dateplaced,
                            COALESCE(
                                sk_dateid_dateremoved,
                                LAST_VALUE(sk_dateid_dateremoved) /*IGNORE NULLS*/ OVER (
                                    PARTITION BY customerid,
                                    symbol ORDER BY w_dts
                                )
                            ) AS sk_dateid_dateremoved,
                            COALESCE(dateplaced, LAST_VALUE(dateplaced) /*IGNORE NULLS*/ OVER (
                                PARTITION BY customerid, symbol ORDER BY w_dts
                            )) AS dateplaced,
                            w_dts,
                            COALESCE(batchid, LAST_VALUE(batchid) /*IGNORE NULLS*/ OVER (
                                PARTITION BY customerid, symbol ORDER BY w_dts
                            )) AS batchid
                        FROM
                            (
                                SELECT
                                    wh.w_c_id AS customerid,
                                    wh.w_s_symb AS symbol,
                                    CASE
                                        WHEN
                                            wh.w_action = 'ACTV'
                                            THEN d.sk_dateid
                                    END AS sk_dateid_dateplaced,
                                    CASE
                                        WHEN
                                            wh.w_action = 'CNCL'
                                            THEN d.sk_dateid
                                    END AS sk_dateid_dateremoved,
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
                                        FROM {{ ref('WatchHistory') }}
                                        UNION ALL
                                        SELECT
                                            w_c_id,
                                            w_s_symb,
                                            w_dts,
                                            w_action,
                                            batchid
                                        FROM {{ ref('WatchIncremental') }}
                                    ) AS wh
                                    INNER JOIN
                                        {{ ref('DimDate') }} AS d
                                        ON d.datevalue = CAST(wh.w_dts AS DATE)
                            ) AS t0
                    ) AS t1
            ) AS t
        WHERE rownum = 1
    ) AS wh
    --  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) = 1) wh -- noqa: LT05
    -- Converts to LEFT JOINs if this is run as DQ EDITION. On some higher
    -- Scale Factors, a small number of Security symbols or Customer IDs "may"
    -- be missing from DimSecurity/DimCustomer, causing audit check failures.
    --${dq_left_flg}
    LEFT JOIN {{ ref('DimSecurity') }} AS s
        ON
            s.symbol = wh.symbol
            AND wh.dateplaced >= s.effectivedate
            AND wh.dateplaced < s.enddate
    --${dq_left_flg}
    LEFT JOIN {{ ref('DimCustomer') }} AS c
        ON
            wh.customerid = c.customerid
            AND wh.dateplaced >= c.effectivedate
            AND wh.dateplaced < c.enddate;
