{{
    config(
        materialized = 'table'
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
    IF(Enddate = DATE('9999-12-31'), true, false) Iscurrent,
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
        IF(
            Fws.Effectivedate < Dc.Effectivedate,
            Dc.Effectivedate,
            Fws.Effectivedate
        ) Effectivedate,
        IF(Fws.Enddate > Dc.Enddate, Dc.Enddate, Fws.Enddate) Enddate
    FROM (
        SELECT
            Fws.* EXCEPT (Status, Conameorcik),

            COALESCE(
                SAFE_CAST(CASE
                    WHEN
                        CHAR_LENGTH(CAST(Conameorcik AS STRING)) <= 10
                        THEN SAFE_CAST(Conameorcik AS INT64)
                    ELSE
                        null
                END AS STRING)
                ,
                SAFE_CAST(CASE
                    WHEN
                        CHAR_LENGTH(CAST(Conameorcik AS STRING)) > 10
                        OR SAFE_CAST(Conameorcik AS INT64) IS NULL
                        THEN Conameorcik
                    ELSE
                        null
                END
                AS STRING)
            ) AS CIK,
            S.ST_NAME AS Status,
            COALESCE(
                LEAD(Effectivedate) OVER (
                    PARTITION BY Symbol
                    ORDER BY Effectivedate
                ),
                DATE('9999-12-31')
            ) Enddate
        FROM (
            SELECT
                DATE(
                    PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S', SUBSTRING(Value, 1, 15))
                ) AS Effectivedate,
                TRIM(SUBSTRING(Value, 19, 15)) AS Symbol,
                TRIM(SUBSTRING(Value, 34, 6)) AS Issue,
                TRIM(SUBSTRING(Value, 40, 4)) AS Status,
                TRIM(SUBSTRING(Value, 44, 70)) AS Name,
                TRIM(SUBSTRING(Value, 114, 6)) AS Exchangeid,
                CAST(SUBSTRING(Value, 120, 13) AS BIGINT) AS Sharesoutstanding,
                PARSE_DATE(
                    '%E4Y%m%d', CAST(SUBSTRING(Value, 133, 8) AS STRING)
                ) AS Firsttrade,
                PARSE_DATE(
                    '%E4Y%m%d', CAST(SUBSTRING(Value, 141, 8) AS STRING)
                ) AS Firsttradeonexchange,
                CAST(SUBSTRING(Value, 149, 12) AS FLOAT64) AS Dividend,
                TRIM(SUBSTRING(Value, 161, 60)) AS Conameorcik


            FROM {{ ref('FinWire') }} WHERE Rectype = "SEC"

        ) Fws
            JOIN {{ source(var('benchmark'), 'StatusType') }} S
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
                CAST(Companyid AS STRING) Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('DimCompany') }}
        ) Dc
            ON
                Fws.CIK = Dc.Conameorcik
                AND Fws.EffectiveDate < Dc.EndDate
                AND Fws.EndDate > Dc.EffectiveDate
) Fws
WHERE Effectivedate != Enddate
