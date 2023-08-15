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
    fmh.sk_dateid,
    fmh.dm_close / NULLIF(f.sum_fi_basic_eps, 0) AS peratio,
    (s.dividend / fmh.dm_close) / 100 AS yield,
    fmh.fiftytwoweekhigh,
    fmh.sk_fiftytwoweekhighdate,
    fmh.fiftytwoweeklow,
    fmh.sk_fiftytwoweeklowdate,
    fmh.dm_close AS closeprice,
    fmh.dm_high AS dayhigh,
    fmh.dm_low AS daylow,
    fmh.dm_vol AS volume
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
                        FROM {{ ref('tempdailymarkethistorical') }} AS a
                            --dbo.tempdailymarkethistorical a
                            INNER JOIN
                                {{ ref('tempdailymarkethistorical') }} AS b
                                --JOIN  dbo.tempdailymarkethistorical b
                                ON
                                    a.dm_s_symb = b.dm_s_symb
                                    AND a.fiftytwoweeklow = b.dm_low
                                    --AND b.dm_date between add_months(a.dm_date, -12) AND a.dm_date -- noqa: LT05
                                    AND b.dm_date BETWEEN DATEADD(
                                        MONTH, -12, a.dm_date
                                    ) AND a.dm_date
                            --    JOIN  {{ ref('tempdailymarkethistorical') }} c
                            INNER JOIN
                                {{ ref('tempdailymarkethistorical') }} AS c
                                ON
                                    a.dm_s_symb = c.dm_s_symb
                                    AND a.fiftytwoweekhigh = c.dm_high
                                    --AND c.dm_date between add_months(a.dm_date, -12) AND a.dm_date) dmh -- noqa: LT05
                                    AND c.dm_date BETWEEN DATEADD(
                                        MONTH, -12, a.dm_date
                                    ) AND a.dm_date
                    ) AS dmh
            ) AS t
        WHERE t.rownum = 1
    --QUALIFY ROW_NUMBER() OVER (
    --  PARTITION BY dm_s_symb, dm_date
    --  ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1
    ) AS fmh
    -- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale
    -- Factors, a small number of Security Security symbols are missing from
    -- DimSecurity, causing audit check failures.
    --${dq_left_flg}
    LEFT JOIN {{ ref('dimsecurity') }} AS s
        --LEFT JOIN dbo.DimSecurity s
        ON
            s.symbol = fmh.dm_s_symb
            AND fmh.dm_date >= s.effectivedate
            AND fmh.dm_date < s.enddate
    LEFT JOIN {{ ref('tempsumpfibasiceps') }} AS f
        --LEFT JOIN  dbo.tempSumpFiBasicEps f
        ON
            f.sk_companyid = s.sk_companyid
            --AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
            AND DATE_PART(QUARTER, fmh.dm_date)
            = DATE_PART(QUARTER, f.fi_qtr_start_date)
            AND DATE_PART(YEAR, fmh.dm_date)
            = DATE_PART(YEAR, f.fi_qtr_start_date)
