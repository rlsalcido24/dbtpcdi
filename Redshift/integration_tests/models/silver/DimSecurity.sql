{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='REPLICATE'

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
    --if(enddate = date('9999-12-31'), True, False) iscurrent,
    CASE WHEN Enddate = '9999-12-31'::DATE THEN 1 ELSE 0 END Iscurrent,
    1 Batchid,
    Effectivedate,
    CONCAT(CONCAT(Exchangeid, '-'), Effectivedate) AS Sk_Securityid,
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
        --if(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate,
        CASE
            WHEN
                Fws.Effectivedate < Dc.Effectivedate
                THEN Dc.Effectivedate
            ELSE Fws.Effectivedate
        END Effectivedate,
        --if(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
        CASE
            WHEN Fws.Enddate > Dc.Enddate THEN Dc.Enddate ELSE Fws.Enddate
        END Enddate
    FROM (
        SELECT
            --fws.* except(Status, conameorcik),
            Fws.Effectivedate,
            Fws.Symbol,
            Fws.Issue,
            Fws.Name,
            Fws.Exchangeid,
            Fws.Sharesoutstanding,
            Fws.Firsttrade,
            Fws.Firsttradeonexchange,
            Fws.Dividend,

            COALESCE(CAST(
                CASE
                    WHEN
                        Conameorcik SIMILAR TO '[0-9]+(.[0-9][0-9])?'
                        THEN Conameorcik::INTEGER
                    ELSE null
                END AS VARCHAR
            ),
            Conameorcik) Conameorcik,
            --nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik,
            --ISNULL( convert(varchar,try_cast(conameorcik as bigint)), conameorcik) conameorcik,
            S.ST_NAME AS Status,
            COALESCE(
                LEAD(Effectivedate) OVER (
                    PARTITION BY Symbol
                    ORDER BY Effectivedate
                ),
                --date('9999-12-31')
                TO_DATE('9999-12-31', 'yyyyMMdd')
            ) Enddate
        FROM (
            SELECT
                TO_TIMESTAMP(
                    SUBSTRING(Value, 1, 15), 'YYYYMMDD-HH24MISS'
                ) AS Effectivedate,
                --        convert(date,convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112)) AS effectivedate,
                TRIM(SUBSTRING(Value, 19, 15)) AS Symbol,
                TRIM(SUBSTRING(Value, 34, 6)) AS Issue,
                TRIM(SUBSTRING(Value, 40, 4)) AS Status,
                TRIM(SUBSTRING(Value, 44, 70)) AS Name,
                TRIM(SUBSTRING(Value, 114, 6)) AS Exchangeid,
                CAST(SUBSTRING(Value, 120, 13) AS BIGINT) AS Sharesoutstanding,
                TO_DATE(SUBSTRING(Value, 133, 8), 'yyyyMMdd') AS Firsttrade,
                --        convert(date,substring(value, 133, 8), 112) AS firsttrade,
                TO_DATE(
                    SUBSTRING(Value, 141, 8), 'yyyyMMdd'
                ) AS Firsttradeonexchange,
                --        convert(date,substring(value, 141, 8), 112) AS firsttradeonexchange,
                CAST(SUBSTRING(Value, 149, 12) AS FLOAT) AS Dividend,
                TRIM(SUBSTRING(Value, 161, 60)) AS Conameorcik
            FROM {{ ref('finwire') }}
            --FROM stg.FinWire
            WHERE Rectype = 'SEC'
        ) Fws
            JOIN {{ source('tpcdi', 'StatusType') }} S
                ON S.ST_ID = Fws.Status
    ) Fws
        JOIN (
            SELECT
                Sk_Companyid,
                Name Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('dimcompany') }}
            UNION ALL
            SELECT
                Sk_Companyid,
                --cast(companyid as string) conameorcik,
                CAST(Companyid AS VARCHAR) Conameorcik,
                EffectiveDate,
                EndDate
            FROM {{ ref('dimcompany') }}
        ) Dc
            ON
                Fws.Conameorcik = Dc.Conameorcik
                AND Fws.EffectiveDate < Dc.EndDate
                AND Fws.EndDate > Dc.EffectiveDate
) Fws
--WHERE effectivedate != enddate
WHERE Effectivedate != Enddate
