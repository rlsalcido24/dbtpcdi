{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='HASH(sk_companyid)'

SELECT
    s.sk_securityid,
    s.sk_companyid,
    sk_dateid,
    fmh.dm_close / NULLIF(sum_fi_basic_eps, 0) AS peratio,
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
                SELECT
                    *,
                    ROW_NUMBER()
                        OVER (
                            PARTITION BY dm_s_symb, dm_date
                            ORDER BY
                                sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate
                        )
                    AS rownum
                FROM
                    (
                        SELECT
                            a.*,
                            b.sk_dateid AS sk_fiftytwoweeklowdate,
                            c.sk_dateid AS sk_fiftytwoweekhighdate
                        FROM
                            {{ ref('tempdailymarkethistorical') }} a
                            --dbo.tempdailymarkethistorical a
                            JOIN {{ ref('tempdailymarkethistorical') }} b
                                --JOIN  dbo.tempdailymarkethistorical b 
                                ON
                                    a.dm_s_symb = b.dm_s_symb
                                    AND a.fiftytwoweeklow = b.dm_low
                                    --AND b.dm_date between add_months(a.dm_date, -12) AND a.dm_date
                                    AND b.dm_date BETWEEN DATEADD(
                                        MONTH, -12, a.dm_date
                                    ) AND a.dm_date
                            --    JOIN  {{ ref('tempdailymarkethistorical') }} c 
                            JOIN {{ ref('tempdailymarkethistorical') }} c

                                ON
                                    a.dm_s_symb = c.dm_s_symb
                                    AND a.fiftytwoweekhigh = c.dm_high
                                    --AND c.dm_date between add_months(a.dm_date, -12) AND a.dm_date) dmh
                                    AND c.dm_date BETWEEN DATEADD(
                                        MONTH, -12, a.dm_date
                                    ) AND a.dm_date
                    ) dmh
            ) t
        WHERE t.rownum = 1
    --QUALIFY ROW_NUMBER() OVER (
    --  PARTITION BY dm_s_symb, dm_date 
    --  ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1
    ) fmh

    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security Security symbols are missing from DimSecurity, causing audit check failures. 
    --${dq_left_flg} 
    LEFT JOIN {{ ref('dimsecurity') }} s
        --LEFT JOIN dbo.DimSecurity s 
        ON
            s.symbol = fmh.dm_s_symb
            AND fmh.dm_date >= s.effectivedate
            AND fmh.dm_date < s.enddate
    LEFT JOIN {{ ref('tempsumpfibasiceps') }} f
        --LEFT JOIN  dbo.tempSumpFiBasicEps f 
        ON
            f.sk_companyid = s.sk_companyid
            --AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
            AND DATE_PART(QUARTER, fmh.dm_date)
            = DATE_PART(QUARTER, fi_qtr_start_date)
            AND DATE_PART(YEAR, fmh.dm_date)
            = DATE_PART(YEAR, fi_qtr_start_date)
