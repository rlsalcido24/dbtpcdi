{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(tradeid)'
    )
}}
-- !!!!! IGNORE NULLS is not supported in Synapse !!!!!
SELECT
    trade.tradeid,
    ds.sk_brokerid,
    trade.sk_createdateid,
    trade.sk_createtimeid,
    trade.sk_closedateid,
    trade.sk_closetimeid,
    status.st_name AS status,
    tt.tt_name AS type, -- noqa: RF04
    trade.cashflag,
    da.sk_securityid,
    da.sk_companyid,
    trade.quantity,
    trade.bidprice,
    da.sk_customerid,
    da.sk_accountid,
    trade.executedby,
    trade.tradeprice,
    trade.fee,
    trade.commission,
    trade.tax,
    trade.batchid
FROM (
    SELECT *
    FROM
        (
            SELECT
                tradeid,
                createdate,
                sk_createdateid,
                sk_createtimeid,
                sk_closedateid,
                sk_closetimeid,
                cashflag,
                t_st_id,
                t_tt_id,
                t_s_symb,
                quantity,
                bidprice,
                t_ca_id,
                executedby,
                tradeprice,
                fee,
                commission,
                tax,
                batchid,
                ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC)
                    AS rownum
            FROM
                (
                    SELECT
                        tradeid,
                        MIN(CAST(t_dts AS DATE))
                            OVER (PARTITION BY tradeid)
                            AS createdate,
                        t_dts,
                        COALESCE(
                            sk_createdateid,
                            LAST_VALUE(sk_createdateid) /*IGNORE NULLS*/ OVER (
                                PARTITION BY tradeid ORDER BY t_dts
                            )
                        ) AS sk_createdateid,
                        COALESCE(
                            sk_createtimeid,
                            LAST_VALUE(sk_createtimeid) /*IGNORE NULLS*/ OVER (
                                PARTITION BY tradeid ORDER BY t_dts
                            )
                        ) AS sk_createtimeid,
                        COALESCE(
                            sk_closedateid,
                            LAST_VALUE(sk_closedateid) /*IGNORE NULLS*/ OVER (
                                PARTITION BY tradeid ORDER BY t_dts
                            )
                        ) AS sk_closedateid,
                        COALESCE(
                            sk_closetimeid,
                            LAST_VALUE(sk_closetimeid) /*IGNORE NULLS*/ OVER (
                                PARTITION BY tradeid ORDER BY t_dts
                            )
                        ) AS sk_closetimeid,
                        cashflag,
                        t_st_id,
                        t_tt_id,
                        t_s_symb,
                        quantity,
                        bidprice,
                        t_ca_id,
                        executedby,
                        tradeprice,
                        fee,
                        commission,
                        tax,
                        batchid
                    FROM
                        (
                            SELECT
                                t.tradeid,
                                t.t_dts,
                                CASE
                                    WHEN t.create_flg > 0 THEN dd.sk_dateid ELSE
                                        CAST(NULL AS BIGINT)
                                END AS sk_createdateid,
                                CASE
                                    WHEN t.create_flg > 0 THEN dt.sk_timeid ELSE
                                        CAST(NULL AS BIGINT)
                                END AS sk_createtimeid,
                                CASE
                                    WHEN t.create_flg = 0 THEN dd.sk_dateid ELSE
                                        CAST(NULL AS BIGINT)
                                END AS sk_closedateid,
                                CASE
                                    WHEN t.create_flg = 0 THEN dt.sk_timeid ELSE
                                        CAST(NULL AS BIGINT)
                                END AS sk_closetimeid,
                                CASE
                                    WHEN t.t_is_cash = 1 THEN CAST(1 AS BIT)
                                    WHEN t.t_is_cash = 0 THEN CAST(0 AS BIT)
                                    ELSE CAST(NULL AS BIT)
                                END AS cashflag,
                                t.t_st_id,
                                t.t_tt_id,
                                t.t_s_symb,
                                t.quantity,
                                t.bidprice,
                                t.t_ca_id,
                                t.executedby,
                                t.tradeprice,
                                t.fee,
                                t.commission,
                                t.tax,
                                t.batchid
                            FROM (
                                SELECT
                                    t.t_id AS tradeid,
                                    th.th_dts AS t_dts,
                                    t.t_st_id,
                                    t.t_tt_id,
                                    t.t_is_cash,
                                    t.t_s_symb,
                                    t.t_qty AS quantity,
                                    t.t_bid_price AS bidprice,
                                    t.t_ca_id,
                                    t.t_exec_name AS executedby,
                                    t.t_trade_price AS tradeprice,
                                    t.t_chrg AS fee,
                                    t.t_comm AS commission,
                                    t.t_tax AS tax,
                                    1 AS batchid,
                                    CASE
                                        WHEN
                                            (
                                                th.th_st_id = 'SBMT'
                                                AND t.t_tt_id IN ('TMB', 'TMS')
                                            )
                                            OR th.th_st_id = 'PNDG'
                                            THEN CAST(1 AS BIT)
                                        WHEN
                                            th.th_st_id IN ('CMPT', 'CNCL')
                                            THEN CAST(0 AS BIT)
                                        ELSE CAST(NULL AS BIT)
                                    END AS create_flg
                                FROM {{ ref('TradeHistory') }} AS t
                                    INNER JOIN
                                        {{ ref('TradeHistoryRaw') }} AS th
                                        ON th.th_t_id = t.t_id
                                UNION ALL
                                SELECT
                                    t_id AS tradeid,
                                    t_dts,
                                    t_st_id,
                                    t_tt_id,
                                    t_is_cash,
                                    t_s_symb,
                                    t_qty AS quantity,
                                    t_bid_price AS bidprice,
                                    t_ca_id,
                                    t_exec_name AS executedby,
                                    t_trade_price AS tradeprice,
                                    t_chrg AS fee,
                                    t_comm AS commission,
                                    t_tax AS tax,
                                    t.batchid,
                                    CASE
                                        WHEN cdc_flag = 'I' THEN CAST(1 AS BIT)
                                        WHEN
                                            t_st_id IN ('CMPT', 'CNCL')
                                            THEN CAST(0 AS BIT)
                                        ELSE CAST(NULL AS BIT)
                                    END AS create_flg
                                FROM {{ ref('TradeIncremental') }} AS t
                            ) AS t
                                INNER JOIN {{ ref('DimDate') }} AS dd
                                    ON CAST(t.t_dts AS DATE) = dd.datevalue
                                INNER JOIN {{ ref('DimTime') }} AS dt
                                    ON CAST(t.t_dts AS TIME) = dt.timevalue
                        ) AS t0
                ) AS t1
        ) AS t2
    WHERE rownum = 1
) AS trade
    INNER JOIN {{ ref('StatusType') }} AS status
        ON status.st_id = trade.t_st_id
    INNER JOIN {{ ref('TradeType') }} AS tt
        ON tt.tt_id = trade.t_tt_id
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Security symbols or Account IDs are missing
    -- from DimSecurity/DimAccount, causing audit check failures.
    --${dq_left_flg}
    LEFT JOIN {{ ref('DimSecurity') }} AS ds
        ON
            ds.symbol = trade.t_s_symb
            AND trade.createdate >= ds.effectivedate
            AND trade.createdate < ds.enddate
    --${dq_left_flg}
    LEFT JOIN {{ ref('DimAccount') }} AS da
        ON
            trade.t_ca_id = da.accountid
            AND trade.createdate >= da.effectivedate
            AND trade.createdate < da.enddate
