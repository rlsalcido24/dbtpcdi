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
        ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) as rownum
    FROM 
    (
        SELECT
            tradeid,
            min(convert(date,t_dts)) OVER (PARTITION BY tradeid) createdate,
            t_dts,
            --coalesce(sk_createdateid, last_value(sk_createdateid) IGNORE NULLS OVER (
                coalesce(sk_createdateid, last_value(sk_createdateid)OVER (
                PARTITION BY tradeid ORDER BY t_dts)) sk_createdateid,
            --coalesce(sk_createtimeid, last_value(sk_createtimeid) IGNORE NULLS OVER (
                coalesce(sk_createtimeid, last_value(sk_createtimeid) OVER (
                PARTITION BY tradeid ORDER BY t_dts)) sk_createtimeid,
            --coalesce(sk_closedateid, last_value(sk_closedateid) IGNORE NULLS OVER (
            coalesce(sk_closedateid, last_value(sk_closedateid) OVER (
                PARTITION BY tradeid ORDER BY t_dts)) sk_closedateid,
            --coalesce(sk_closetimeid, last_value(sk_closetimeid) IGNORE NULLS OVER (
            coalesce(sk_closetimeid, last_value(sk_closetimeid) OVER (
                PARTITION BY tradeid ORDER BY t_dts)) sk_closetimeid,
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
                tradeid,
                t_dts,
                case when create_flg>0 then sk_dateid else cast(NULL AS BIGINT) end sk_createdateid,
                case when create_flg>0 then sk_timeid else cast(NULL AS BIGINT) end sk_createtimeid,
                case when create_flg=0 then sk_dateid else cast(NULL AS BIGINT) end sk_closedateid,
                case when create_flg=0 then sk_timeid else cast(NULL AS BIGINT) end sk_closetimeid,
                CASE 
                WHEN t_is_cash = 1 then cast(1 as bit)
                WHEN t_is_cash = 0 then cast(0 as bit)
                ELSE cast(null as bit) END AS cashflag,
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
                    WHEN (th_st_id = 'SBMT' AND t_tt_id IN ('TMB', 'TMS')) OR th_st_id = 'PNDG' THEN cast(1 as bit) 
                    WHEN th_st_id IN ('CMPT', 'CNCL') THEN cast(0 as bit)
                    ELSE cast(null as bit) END AS create_flg
                FROM {{ ref('TradeHistory') }} t
                JOIN {{ ref('TradeHistoryRaw') }} th
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
                    WHEN cdc_flag = 'I' THEN cast(1 as bit) 
                    WHEN t_st_id IN ('CMPT', 'CNCL') THEN cast(0 as bit) 
                    ELSE cast(null as bit) END AS create_flg
                FROM {{ ref('TradeIncremental') }} t
            ) t
            JOIN {{ ref('DimDate') }} dd
                ON convert(date,t.t_dts) = dd.datevalue
            JOIN {{ ref('DimTime') }} dt
                ON convert(time,t.t_dts) = dt.timevalue
        ) T0
    ) T1
  ) T2
  WHERE T2.rownum=1
--  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
) trade
JOIN {{ ref('StatusType') }} status
  ON status.st_id = trade.t_st_id
JOIN {{ ref('TradeType') }} tt
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