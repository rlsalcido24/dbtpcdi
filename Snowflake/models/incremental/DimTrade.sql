{{
    config(
        materialized = 'table'
    )
}}
SELECT
    trade.tradeid,
    sk_brokerid,
    trade.sk_createdateid,
    trade.sk_createtimeid,
    trade.sk_closedateid,
    trade.sk_closetimeid,
    st_name status,
    tt_name type,
    trade.cashflag,
    sk_securityid,
    sk_companyid,
    trade.quantity,
    trade.bidprice,
    sk_customerid,
    sk_accountid,
    trade.executedby,
    trade.tradeprice,
    trade.fee,
    trade.commission,
    trade.tax,
    trade.batchid
FROM (
    SELECT * EXCLUDE (t_dts)
    FROM (
        SELECT
            tradeid,
            MIN(DATE(t_dts)) OVER (PARTITION BY tradeid) createdate,
            t_dts,
            COALESCE(
                sk_createdateid, LAST_VALUE(sk_createdateid) IGNORE NULLS OVER (
                    PARTITION BY tradeid ORDER BY t_dts
                )
            ) sk_createdateid,
            COALESCE(
                sk_createtimeid, LAST_VALUE(sk_createtimeid) IGNORE NULLS OVER (
                    PARTITION BY tradeid ORDER BY t_dts
                )
            ) sk_createtimeid,
            COALESCE(
                sk_closedateid, LAST_VALUE(sk_closedateid) IGNORE NULLS OVER (
                    PARTITION BY tradeid ORDER BY t_dts
                )
            ) sk_closedateid,
            COALESCE(
                sk_closetimeid, LAST_VALUE(sk_closetimeid) IGNORE NULLS OVER (
                    PARTITION BY tradeid ORDER BY t_dts
                )
            ) sk_closetimeid,
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
        FROM (
            SELECT
                tradeid,
                t_dts,
                IFF(create_flg, sk_dateid, CAST(null AS BIGINT))
                    sk_createdateid,
                IFF(create_flg, sk_timeid, CAST(null AS BIGINT))
                    sk_createtimeid,
                IFF(create_flg, sk_dateid, CAST(null AS BIGINT)) sk_closedateid,
                IFF(create_flg, sk_timeid, CAST(null AS BIGINT)) sk_closetimeid,
                CASE
                    WHEN t_is_cash = 1 THEN true
                    WHEN t_is_cash = 0 THEN false
                    ELSE CAST(null AS BOOLEAN)
                END AS cashflag,
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
                t.batchid
            FROM (
                SELECT
                    t_id tradeid,
                    th_dts t_dts,
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
                    1 batchid,
                    CASE
                        WHEN
                            (th_st_id = 'SBMT' AND t_tt_id IN ('TMB', 'TMS'))
                            OR th_st_id = 'PNDG'
                            THEN true
                        WHEN th_st_id IN ('CMPT', 'CNCL') THEN false
                        ELSE CAST(null AS BOOLEAN)
                    END AS create_flg
                FROM {{ source('tpcdi', 'TradeHistory') }} t
                    JOIN {{ source('tpcdi', 'TradeHistoryRaw') }} th
                        ON th_t_id = t_id
                UNION ALL
                SELECT
                    t_id tradeid,
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
                        WHEN cdc_flag = 'I' THEN true
                        WHEN t_st_id IN ('CMPT', 'CNCL') THEN false
                        ELSE CAST(null AS BOOLEAN)
                    END AS create_flg
                FROM {{ ref('TradeIncremental') }} t
            ) t
                JOIN {{ source('tpcdi', 'DimDate') }} dd
                    ON DATE(t.t_dts) = dd.datevalue
                JOIN {{ source('tpcdi', 'DimTime') }} dt
                    ON TO_CHAR(t.t_dts, 'hh:mi:ss') = dt.timevalue
        )
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC) = 1
) trade
    JOIN {{ source('tpcdi', 'StatusType') }} status
        ON status.st_id = trade.t_st_id
    JOIN {{ source('tpcdi', 'TradeType') }} tt
        ON tt.tt_id = trade.t_tt_id
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security symbols or Account IDs are missing from DimSecurity/DimAccount, causing audit check failures. 
    --${dq_left_flg} 
    LEFT JOIN {{ ref('DimSecurity') }} ds
        ON
            ds.symbol = trade.t_s_symb
            AND createdate >= ds.effectivedate
            AND createdate < ds.enddate
    --${dq_left_flg} 
    LEFT JOIN {{ ref('DimAccount') }} da
        ON
            trade.t_ca_id = da.accountid
            AND createdate >= da.effectivedate
            AND createdate < da.enddate
