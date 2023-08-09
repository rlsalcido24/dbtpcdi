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
    st_name AS status,
    tt_name AS type, --noqa: RF04
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
    SELECT * EXCEPT (t_dts)
    FROM (
        SELECT
            tradeid,
            MIN(DATE(t_dts)) OVER (PARTITION BY tradeid) AS createdate,
            t_dts,
            COALESCE(
                sk_createdateid,
                LAST_VALUE(sk_createdateid IGNORE NULLS)
                    OVER (
                        PARTITION BY tradeid
                        ORDER BY
                            t_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS sk_createdateid,
            COALESCE(
                sk_createtimeid,
                LAST_VALUE(sk_createtimeid IGNORE NULLS)
                    OVER (
                        PARTITION BY tradeid
                        ORDER BY
                            t_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS sk_createtimeid,
            COALESCE(
                sk_closedateid,
                LAST_VALUE(sk_closedateid IGNORE NULLS)
                    OVER (
                        PARTITION BY tradeid
                        ORDER BY
                            t_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
            ) AS sk_closedateid,
            COALESCE(
                sk_closetimeid,
                LAST_VALUE(sk_closetimeid IGNORE NULLS)
                    OVER (
                        PARTITION BY tradeid
                        ORDER BY
                            t_dts
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
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
        FROM (
            SELECT
                t.tradeid,
                t.t_dts,
                IF(
                    t.create_flg,
                    sk_dateid,
                    CAST(null AS BIGINT)
                ) AS sk_createdateid,
                IF(
                    t.create_flg,
                    sk_timeid,
                    CAST(null AS BIGINT)
                ) AS sk_createtimeid,
                IF(
                    t.create_flg,
                    sk_dateid,
                    CAST(null AS BIGINT)
                ) AS sk_closedateid,
                IF(
                    t.create_flg,
                    sk_timeid,
                    CAST(null AS BIGINT)
                ) AS sk_closetimeid,
                CASE
                    WHEN t.t_is_cash = 1 THEN true
                    WHEN t.t_is_cash = 0 THEN false
                    ELSE
                        CAST(null AS BOOLEAN)
                END
                    AS cashflag,
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
                            THEN true
                        WHEN th.th_st_id IN (
                            'CMPT',
                            'CNCL'
                        ) THEN false
                        ELSE
                            CAST(null AS BOOLEAN)
                    END
                        AS create_flg
                FROM
                    {{ source(var('benchmark'),'TradeHistory') }} AS t
                    INNER JOIN
                        {{ source(var('benchmark'),'TradeHistoryRaw') }} AS th
                        ON
                            th.th_t_id = t.t_id
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
                        WHEN cdc_flag = 'I' THEN true
                        WHEN t_st_id IN (
                            'CMPT',
                            'CNCL'
                        ) THEN false
                        ELSE
                            CAST(null AS BOOLEAN)
                    END
                        AS create_flg
                FROM
                    {{ ref('TradeIncremental') }} AS t
            ) AS t
                INNER JOIN
                    {{ source(var('benchmark'),'DimDate') }} AS dd
                    ON
                        DATE(t.t_dts) = dd.datevalue
                INNER JOIN
                    {{ source(var('benchmark'),'DimTime') }} AS dt
                    ON
                        FORMAT_TIMESTAMP('%H:%M:%S', t.t_dts) = dt.timevalue
        )
    ) QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC) = 1
) AS trade
    INNER JOIN
        {{ source(var('benchmark'),'StatusType') }} AS status
        ON
            status.st_id = trade.t_st_id
    INNER JOIN
        {{ source(var('benchmark'),'TradeType') }} AS tt
        ON
            tt.tt_id = trade.t_tt_id
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Security symbols or Account IDs are missing
    -- from DimSecurity/DimAccount, causing audit check failures.
    --${dq_left_flg}
    LEFT JOIN
        {{ ref('DimSecurity') }} AS ds
        ON
            ds.symbol = trade.t_s_symb
            AND trade.createdate >= ds.effectivedate
            AND trade.createdate < ds.enddate
    --${dq_left_flg}
    LEFT JOIN
        {{ ref('DimAccount') }} AS da
        ON
            trade.t_ca_id = da.accountid
            AND trade.createdate >= da.effectivedate
            AND trade.createdate < da.enddate
