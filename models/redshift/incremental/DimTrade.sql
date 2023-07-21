{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='REPLICATE'



SELECT
  trade.tradeid,
  sk_brokerid,
  trade.sk_createdateid,
  trade.sk_createtimeid,
  trade.sk_closedateid,
  trade.sk_closetimeid,
  st_name as status,
  tt_name as type,
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
        --* EXCEPT(t_dts)
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
            min(to_date(t_dts,'YYYYMMDD')) OVER (PARTITION BY tradeid) createdate,
            t_dts,
            --coalesce(sk_createdateid, last_value(sk_createdateid) IGNORE NULLS OVER (
                coalesce(sk_createdateid, last_value(sk_createdateid)OVER (
                PARTITION BY tradeid ORDER BY t_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_createdateid,
            --coalesce(sk_createtimeid, last_value(sk_createtimeid) IGNORE NULLS OVER (
                coalesce(sk_createtimeid, last_value(sk_createtimeid) OVER (
                PARTITION BY tradeid ORDER BY t_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_createtimeid,
            --coalesce(sk_closedateid, last_value(sk_closedateid) IGNORE NULLS OVER (
            coalesce(sk_closedateid, last_value(sk_closedateid) OVER (
                PARTITION BY tradeid ORDER BY t_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_closedateid,
            --coalesce(sk_closetimeid, last_value(sk_closetimeid) IGNORE NULLS OVER (
            coalesce(sk_closetimeid, last_value(sk_closetimeid) OVER (
                PARTITION BY tradeid ORDER BY t_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_closetimeid,
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
                --if(create_flg, sk_dateid, cast(NULL AS BIGINT)) sk_createdateid,
                case when create_flg>0 then cast(sk_dateid as bigint) else cast(NULL AS BIGINT) end sk_createdateid,
                --if(create_flg, sk_timeid, cast(NULL AS BIGINT)) sk_createtimeid,
                case when create_flg>0 then cast(sk_dateid as bigint) else cast(NULL AS BIGINT) end sk_createtimeid,
                --if(!create_flg, sk_dateid, cast(NULL AS BIGINT)) sk_closedateid,
                case when create_flg=0 then cast(sk_dateid as bigint) else cast(NULL AS BIGINT) end sk_closedateid,
                --if(!create_flg, sk_timeid, cast(NULL AS BIGINT)) sk_closetimeid,
                case when create_flg=0 then cast(sk_dateid as bigint) else cast(NULL AS BIGINT) end sk_closetimeid,
                CASE 
                WHEN t_is_cash = 1 then cast(1 as BOOLEAN)
                WHEN t_is_cash = 0 then cast(0 as BOOLEAN)
                --ELSE cast(null as BOOLEAN) END AS cashflag,
                ELSE cast(null as BOOLEAN) END AS cashflag,
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
                    WHEN (th_st_id = 'SBMT' AND t_tt_id IN ('TMB', 'TMS')) OR th_st_id = 'PNDG' THEN 1
                    WHEN th_st_id IN ('CMPT', 'CNCL') THEN 0
                    ELSE 0 END AS create_flg
                FROM {{ source('tpcdi', 'TradeHistory') }} t
                --FROM prd.TradeHistory t
                JOIN {{ source('tpcdi', 'TradeHistoryRaw') }} th
                --JOIN prd.TradeHistoryRaw th
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
                    WHEN cdc_flag = 'I' THEN 1
                    WHEN t_st_id IN ('CMPT', 'CNCL') THEN 0
                    ELSE 0 END AS create_flg
                FROM {{ ref('tradeincremental') }} t
                --FROM stg.TradeIncremental t
            ) t
            JOIN {{ source('tpcdi', 'DimDate') }} dd
            --JOIN prd.DimDate dd
                --ON date(t.t_dts) = dd.datevalue
                ON to_date(t.t_dts,'YYYYMMDD') = dd.datevalue
            JOIN {{ source('tpcdi', 'DimTime') }} dt
            --JOIN prd.DimTime dt
                --ON date_format(t.t_dts, 'HH:mm:ss') = dt.timevalue
                ON TO_CHAR(t.t_dts, 'HH24:MI:SS') = dt.timevalue
        ) T0
    ) T1
  ) T2
  WHERE T2.rownum=1
--  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
) trade
JOIN {{ source('tpcdi', 'StatusType') }} statustype
--JOIN prd.StatusType status
  ON statustype.st_id = trade.t_st_id
JOIN {{ source('tpcdi', 'TradeType') }} tt
--JOIN prd.TradeType tt
  ON tt.tt_id = trade.t_tt_id
-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security symbols or Account IDs are missing from DimSecurity/DimAccount, causing audit check failures. 
--${dq_left_flg} 
LEFT JOIN {{ ref('dimsecurity') }} ds
--LEFT JOIN dbo.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND createdate >= ds.effectivedate 
    AND createdate < ds.enddate
--${dq_left_flg} 
LEFT JOIN {{ ref('dimaccount') }} da
--LEFT JOIN dbo.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND createdate >= da.effectivedate 
    AND createdate < da.enddate