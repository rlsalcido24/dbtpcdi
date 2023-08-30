{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(sk_companyid)'
    )
}}


SELECT 
  s.sk_securityid,
  s.sk_companyid,
  sk_dateid,
  fmh.dm_close / nullif(sum_fi_basic_eps,0) AS peratio,
  (s.dividend / fmh.dm_close) / 100 yield,
  fiftytwoweekhigh,
  sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  sk_fiftytwoweeklowdate,
  dm_close closeprice,
  dm_high dayhigh,
  dm_low daylow,
  dm_vol volume
FROM 
(
    SELECT *
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY dm_s_symb, dm_date ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) as rownum
        FROM 
        (
            SELECT 
            a.*,
            b.sk_dateid AS sk_fiftytwoweeklowdate,
            c.sk_dateid AS sk_fiftytwoweekhighdate
            FROM
              {{ ref('tempDailyMarketHistorical') }} a
            JOIN  {{ ref('tempDailyMarketHistorical') }} b 
            ON
                a.dm_s_symb = b.dm_s_symb
                AND a.fiftytwoweeklow = b.dm_low
                AND b.dm_date between DATEADD(month,-12,a.dm_date) AND a.dm_date
            JOIN  {{ ref('tempDailyMarketHistorical') }} c 
            ON 
                a.dm_s_symb = c.dm_s_symb
                AND a.fiftytwoweekhigh = c.dm_high
                AND c.dm_date between DATEADD(month, -12, a.dm_date) AND a.dm_date
        ) dmh
    ) T
    WHERE T.rownum=1
  --QUALIFY ROW_NUMBER() OVER (
  --  PARTITION BY dm_s_symb, dm_date 
  --  ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1
) fmh

-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security Security symbols are missing from DimSecurity, causing audit check failures. 
--${dq_left_flg} 
LEFT JOIN {{ ref('DimSecurity') }} s 
  ON 
    s.symbol = fmh.dm_s_symb
    AND fmh.dm_date >= s.effectivedate 
    AND fmh.dm_date < s.enddate
LEFT JOIN  {{ ref('tempSumpFiBasicEps') }} f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND DATEPART(quarter,fmh.dm_date) = DATEPART(quarter,fi_qtr_start_date)
    AND year(fmh.dm_date) = year(fi_qtr_start_date);
