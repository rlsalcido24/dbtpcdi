{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
       --, dist='HASH(sk_customerid)'



SELECT
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  sk_dateid_dateplaced,
  sk_dateid_dateremoved,
  wh.batchid
FROM
(
    SELECT 
        *
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
            ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) rownum
        FROM 
        (
            SELECT
                customerid,
                symbol,
                --coalesce(sk_dateid_dateplaced, last_value(sk_dateid_dateplaced) IGNORE NULLS OVER (
                coalesce(sk_dateid_dateplaced, last_value(sk_dateid_dateplaced) OVER (
                    PARTITION BY customerid, symbol ORDER BY w_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_dateid_dateplaced,
                --coalesce(sk_dateid_dateremoved, last_value(sk_dateid_dateremoved) IGNORE NULLS OVER (
                coalesce(sk_dateid_dateremoved, last_value(sk_dateid_dateremoved) OVER (
                    PARTITION BY customerid, symbol ORDER BY w_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) sk_dateid_dateremoved,
                --coalesce(dateplaced, last_value(dateplaced) IGNORE NULLS OVER (
                coalesce(dateplaced, last_value(dateplaced) OVER (
                    PARTITION BY customerid, symbol ORDER BY w_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) dateplaced,
                w_dts,
                --coalesce(batchid, last_value(batchid) IGNORE NULLS OVER (
                coalesce(batchid, last_value(batchid) OVER (
                    PARTITION BY customerid, symbol ORDER BY w_dts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) batchid
            FROM 
            ( 
                SELECT 
                    wh.w_c_id customerid,
                    wh.w_s_symb symbol,
                    --if(w_action = 'ACTV', d.sk_dateid, null) sk_dateid_dateplaced,
                    case when w_action = 'ACTV' then d.sk_dateid else null end sk_dateid_dateplaced,
                    --if(w_action = 'CNCL', d.sk_dateid, null) sk_dateid_dateremoved,
                    case when w_action = 'CNCL' then d.sk_dateid else null end sk_dateid_dateremoved,
                    --if(w_action = 'ACTV', d.datevalue, null) dateplaced,
                    case when w_action = 'ACTV' then d.datevalue else null end dateplaced,
                    wh.w_dts,
                    batchid 
                FROM 
                (
                    SELECT *, 1 batchid 
                    FROM {{ source('tpcdi', 'WatchHistory') }}
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
                ) wh
                JOIN {{ source('tpcdi', 'DimDate') }} d ON d.datevalue = to_date(wh.w_dts,'YYYYMMDD')
                --JOIN prd.DimDate d ON d.datevalue = convert(date,wh.w_dts)
            ) T0
        ) T1
    ) T
    WHERE T.rownum = 1
) wh
--  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) = 1) wh
-- Converts to LEFT JOINs if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security symbols or Customer IDs "may" be missing from DimSecurity/DimCustomer, causing audit check failures. 
--${dq_left_flg} 
LEFT JOIN {{ ref('dimsecurity') }} s 
--LEFT JOIN dbo.DimSecurity s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
--${dq_left_flg} 
LEFT JOIN {{ ref('dimcustomer') }} c 
--LEFT JOIN dbo.DimCustomer c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate


