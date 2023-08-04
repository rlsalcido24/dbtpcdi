{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(symbol)'
    )
}}


SELECT
    Symbol,
    Issue,
    Status,
    Name,
    Exchangeid,
    Sk_Companyid,
    Sharesoutstanding,
    Firsttrade,
    Firsttradeonexchange,
    Dividend,
    CASE WHEN Enddate = CONVERT(DATE, '9999-12-31') THEN 1 ELSE 0 END Iscurrent,
    1 Batchid,
    Effectivedate,
    CONCAT(Exchangeid, '-', Effectivedate) AS Sk_Securityid,
    Enddate
FROM (
    SELECT
        Fws.Symbol,
        Fws.Issue,
        Fws.Status,
        Fws.Name,
        Fws.Exchangeid,
        Dc.Sk_Companyid,
        Fws.Sharesoutstanding,
        Fws.Firsttrade,
        Fws.Firsttradeonexchange,
        Fws.Dividend,
        CASE
            WHEN
                Fws.Effectivedate < Dc.Effectivedate
                THEN Dc.Effectivedate
            ELSE Fws.Effectivedate
        END Effectivedate,
        CASE
            WHEN Fws.Enddate > Dc.Enddate THEN Dc.Enddate ELSE Fws.Enddate
        END Enddate
    FROM (
        SELECT
            Fws.Effectivedate,
            Fws.Symbol,
            Fws.Issue,
            Fws.Name,
            Fws.Exchangeid,
            Fws.Sharesoutstanding,
            Fws.Firsttrade,
            Fws.Firsttradeonexchange,
            Fws.Dividend,
            ISNULL(
                CONVERT(VARCHAR, TRY_CAST(Conameorcik AS BIGINT)), Conameorcik
            ) Conameorcik,
            S.ST_NAME AS Status,
            COALESCE(
                LEAD(Effectivedate) OVER (
                    PARTITION BY Symbol
                    ORDER BY Effectivedate
                ),
                CONVERT(DATE, '9999-12-31')
            ) Enddate
        FROM (
            SELECT
                CONVERT(
                    DATE,
                    CONVERT(
                        DATETIME2,
                        SUBSTRING([value], 1, 8)
                        + ' '
                        + SUBSTRING([value], 10, 2)
                        + ':'
                        + SUBSTRING([value], 12, 2)
                        + ':'
                        + SUBSTRING([value], 14, 2),
                        112
                    )
                ) AS Effectivedate,
                TRIM(SUBSTRING(Value, 19, 15)) AS Symbol,
                TRIM(SUBSTRING(Value, 34, 6)) AS Issue,
                TRIM(SUBSTRING(Value, 40, 4)) AS Status,
                TRIM(SUBSTRING(Value, 44, 70)) AS Name,
                TRIM(SUBSTRING(Value, 114, 6)) AS Exchangeid,
                CAST(SUBSTRING(Value, 120, 13) AS BIGINT) AS Sharesoutstanding,
                CONVERT(DATE, SUBSTRING(Value, 133, 8), 112) AS Firsttrade,
                CONVERT(DATE, SUBSTRING(Value, 141, 8), 112)
                    AS Firsttradeonexchange,
                CAST(SUBSTRING(Value, 149, 12) AS DECIMAL(10, 2)) AS Dividend,
                TRIM(SUBSTRING(Value, 161, 60)) AS Conameorcik
            FROM {{ ref('FinWire_SEC') }}
        ) Fws
            JOIN {{ ref('StatusType') }} S
                ON S.ST_ID = Fws.Status
    ) Fws
        JOIN (
            SELECT
                Sk_Companyid,
                Name Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                Sk_Companyid,
                CAST(Companyid AS VARCHAR) Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('DimCompany') }}
        ) Dc
            ON
                Fws.Conameorcik = Dc.Conameorcik
                AND Fws.EffectiveDate < Dc.EndDate
                AND Fws.EndDate > Dc.EffectiveDate
) Fws
WHERE Effectivedate != Enddate
