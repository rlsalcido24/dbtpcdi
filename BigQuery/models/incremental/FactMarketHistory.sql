{{
    config(
        materialized = 'table'
    )
}}
SELECT -- noqa: ST06
    s.sk_securityid,
    s.sk_companyid,
    sk_dateid,
    fmh.dm_close / NULLIF(sum_fi_basic_eps, 0) AS peratio,
    (s.dividend / fmh.dm_close) / 100 AS yield,
    fiftytwoweekhigh,
    sk_fiftytwoweekhighdate,
    fiftytwoweeklow,
    sk_fiftytwoweeklowdate,
    dm_close AS closeprice,
    dm_high AS dayhigh,
    dm_low AS daylow,
    dm_vol AS volume
FROM (
    SELECT *
    FROM (
        SELECT
            a.*,
            b.sk_dateid AS sk_fiftytwoweeklowdate,
            c.sk_dateid AS sk_fiftytwoweekhighdate
        FROM
            {{ ref('tempDailyMarketHistorical') }} AS a
            INNER JOIN
                {{ ref('tempDailyMarketHistorical') }} AS b
                ON
                    a.dm_s_symb = b.dm_s_symb
                    AND a.fiftytwoweeklow = b.dm_low
                    AND b.dm_date BETWEEN DATE_SUB(a.dm_date, INTERVAL 12 MONTH)
                    AND a.dm_date
            INNER JOIN
                {{ ref('tempDailyMarketHistorical') }} AS c
                ON
                    a.dm_s_symb = c.dm_s_symb
                    AND a.fiftytwoweekhigh = c.dm_high
                    AND c.dm_date BETWEEN DATE_SUB(a.dm_date, INTERVAL 12 MONTH)
                    AND a.dm_date
    ) AS dmh QUALIFY
        ROW_NUMBER()
            OVER (
                PARTITION BY dm_s_symb, dm_date
                ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate
            )
        = 1
) AS fmh
-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors,
-- a small number of Security Security symbols are missing from DimSecurity,
-- causing audit check failures.
--${dq_left_flg}
    LEFT JOIN
        {{ ref('DimSecurity') }} AS s
        ON
            s.symbol = fmh.dm_s_symb
            AND fmh.dm_date >= s.effectivedate
            AND fmh.dm_date < s.enddate
    LEFT JOIN
        {{ ref('tempSumpFiBasicEps') }} AS f
        ON
            f.sk_companyid = s.sk_companyid
            AND EXTRACT(QUARTER FROM fmh.dm_date)
            = EXTRACT(QUARTER FROM fi_qtr_start_date)
            AND EXTRACT(YEAR FROM fmh.dm_date)
            = EXTRACT(YEAR FROM fi_qtr_start_date)
