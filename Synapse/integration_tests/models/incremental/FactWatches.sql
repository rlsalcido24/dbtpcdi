{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        , dist='HASH(sk_customerid)'
    )
}}

-- !!!!! IGNORE NULLS is not supported in Synapse !!!!!

SELECT
    c.sk_customerid sk_customerid,
    s.sk_securityid sk_securityid,
    sk_dateid_dateplaced,
    sk_dateid_dateremoved,
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
                    ) rownum
                FROM
                    (
                        SELECT
                            customerid,
                            symbol,
                            --coalesce(sk_dateid_dateplaced, last_value(sk_dateid_dateplaced) IGNORE NULLS OVER (
                            COALESCE(
                                sk_dateid_dateplaced,
                                LAST_VALUE(sk_dateid_dateplaced) OVER (
                                    PARTITION BY customerid,
                                    symbol ORDER BY w_dts
                                )
                            ) sk_dateid_dateplaced,
                            --coalesce(sk_dateid_dateremoved, last_value(sk_dateid_dateremoved) IGNORE NULLS OVER (
                            COALESCE(
                                sk_dateid_dateremoved,
                                LAST_VALUE(sk_dateid_dateremoved) OVER (
                                    PARTITION BY customerid,
                                    symbol ORDER BY w_dts
                                )
                            ) sk_dateid_dateremoved,
                            --coalesce(dateplaced, last_value(dateplaced) IGNORE NULLS OVER (
                            COALESCE(dateplaced, LAST_VALUE(dateplaced) OVER (
                                PARTITION BY customerid, symbol ORDER BY w_dts
                            )) dateplaced,
                            w_dts,
                            --coalesce(batchid, last_value(batchid) IGNORE NULLS OVER (
                            COALESCE(batchid, LAST_VALUE(batchid) OVER (
                                PARTITION BY customerid, symbol ORDER BY w_dts
                            )) batchid
                        FROM
                            (
                                SELECT
                                    wh.w_c_id customerid,
                                    wh.w_s_symb symbol,
                                    CASE
                                        WHEN
                                            w_action = 'ACTV'
                                            THEN d.sk_dateid
                                        ELSE NULL
                                    END sk_dateid_dateplaced,
                                    CASE
                                        WHEN
                                            w_action = 'CNCL'
                                            THEN d.sk_dateid
                                        ELSE NULL
                                    END sk_dateid_dateremoved,
                                    CASE
                                        WHEN
                                            w_action = 'ACTV'
                                            THEN d.datevalue
                                        ELSE NULL
                                    END dateplaced,
                                    wh.w_dts,
                                    batchid
                                FROM
                                    (
                                        SELECT
                                            *,
                                            1 batchid
                                        FROM {{ ref('WatchHistory') }}
                                        UNION ALL
                                        SELECT
                                            w_c_id,
                                            w_s_symb,
                                            w_dts,
                                            w_action,
                                            batchid
                                        FROM {{ ref('WatchIncremental') }}
                                    ) wh
                                    JOIN
                                        {{ ref('DimDate') }} d
                                        ON d.datevalue = CONVERT(DATE, wh.w_dts)
                            ) t0
                    ) t1
            ) t
        WHERE t.rownum = 1
    ) wh
    --  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) = 1) wh
    -- Converts to LEFT JOINs if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security symbols or Customer IDs "may" be missing from DimSecurity/DimCustomer, causing audit check failures. 
    --${dq_left_flg} 
    LEFT JOIN {{ ref('DimSecurity') }} s
        ON
            s.symbol = wh.symbol
            AND wh.dateplaced >= s.effectivedate
            AND wh.dateplaced < s.enddate
    --${dq_left_flg} 
    LEFT JOIN {{ ref('DimCustomer') }} c
        ON
            wh.customerid = c.customerid
            AND wh.dateplaced >= c.effectivedate
            AND wh.dateplaced < c.enddate;
