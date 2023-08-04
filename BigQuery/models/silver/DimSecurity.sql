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
    if(Enddate = date('9999-12-31'), True, False) AS Iscurrent,
    1 AS Batchid,
    Effectivedate,
    concat(Exchangeid, '-', Effectivedate) AS Sk_Securityid,
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
        if(
            Fws.Effectivedate < Dc.Effectivedate,
            Dc.Effectivedate,
            Fws.Effectivedate
        ) AS Effectivedate,
        if(Fws.Enddate > Dc.Enddate, Dc.Enddate, Fws.Enddate) AS Enddate
    FROM (
        SELECT -- noqa: ST06
            Fws.* EXCEPT (Status, Conameorcik),

            coalesce(
                safe_cast(CASE
                    WHEN
                        char_length(cast(Conameorcik AS STRING)) <= 10
                        THEN safe_cast(Conameorcik AS INT64)
                END AS STRING)
                ,
                safe_cast(CASE
                    WHEN
                        char_length(cast(Conameorcik AS STRING)) > 10
                        OR safe_cast(Conameorcik AS INT64) IS NULL
                        THEN Conameorcik
                END
                AS STRING)
            ) AS CIK,
            S.ST_NAME AS Status,
            coalesce(
                lead(Effectivedate) OVER (
                    PARTITION BY Symbol
                    ORDER BY Effectivedate
                ),
                date('9999-12-31')
            ) AS Enddate
        FROM (
            SELECT -- noqa: ST06
                date(
                    parse_timestamp('%E4Y%m%d-%H%M%S', substring(Value, 1, 15))
                ) AS Effectivedate,
                trim(substring(Value, 19, 15)) AS Symbol,
                trim(substring(Value, 34, 6)) AS Issue,
                trim(substring(Value, 40, 4)) AS Status,
                trim(substring(Value, 44, 70)) AS Name,  -- noqa: RF04
                trim(substring(Value, 114, 6)) AS Exchangeid,
                cast(substring(Value, 120, 13) AS BIGINT) AS Sharesoutstanding,
                parse_date(
                    '%E4Y%m%d', cast(substring(Value, 133, 8) AS STRING)
                ) AS Firsttrade,
                parse_date(
                    '%E4Y%m%d', cast(substring(Value, 141, 8) AS STRING)
                ) AS Firsttradeonexchange,
                cast(substring(Value, 149, 12) AS FLOAT64) AS Dividend,
                trim(substring(Value, 161, 60)) AS Conameorcik


            FROM {{ ref('FinWire') }} WHERE Rectype = 'SEC'

        ) AS Fws
            INNER JOIN {{ source(var('benchmark'), 'StatusType') }} AS S
                ON S.ST_ID = Fws.Status
    ) AS Fws
        INNER JOIN (
            SELECT
                Sk_Companyid,
                Name AS Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                Sk_Companyid,
                cast(Companyid AS STRING) AS Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('DimCompany') }}
        ) AS Dc
            ON
                Fws.CIK = Dc.Conameorcik
                AND Fws.EffectiveDate < Dc.EndDate
                AND Fws.EndDate > Dc.EffectiveDate
) AS Fws
WHERE Effectivedate != Enddate
