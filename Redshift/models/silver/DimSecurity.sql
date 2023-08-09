{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='REPLICATE'

SELECT
    symbol,
    issue,
    status,
    name,
    exchangeid,
    sk_companyid,
    sharesoutstanding,
    firsttrade,
    firsttradeonexchange,
    dividend,
    --if(enddate = date('9999-12-31'), True, False) iscurrent,
    CASE WHEN enddate = '9999-12-31'::DATE THEN 1 ELSE 0 END AS iscurrent,
    1 AS batchid,
    effectivedate,
    CONCAT(CONCAT(exchangeid, '-'), effectivedate) AS sk_securityid,
    enddate
FROM (
    SELECT
        fws.symbol,
        fws.issue,
        fws.status,
        fws.name,
        fws.exchangeid,
        dc.sk_companyid,
        fws.sharesoutstanding,
        fws.firsttrade,
        fws.firsttradeonexchange,
        fws.dividend,
        --if(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate, -- noqa: LT05
        CASE
            WHEN
                fws.effectivedate < dc.effectivedate
                THEN dc.effectivedate
            ELSE fws.effectivedate
        END AS effectivedate,
        --if(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
        CASE
            WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate
        END AS enddate
    FROM (
        SELECT
            --fws.* except(Status, conameorcik),
            fws.effectivedate,
            fws.symbol,
            fws.issue,
            fws.name,
            fws.exchangeid,
            fws.sharesoutstanding,
            fws.firsttrade,
            fws.firsttradeonexchange,
            fws.dividend,
            COALESCE(CAST(
                CASE
                    WHEN
                        fws.conameorcik SIMILAR TO '[0-9]+(.[0-9][0-9])?'
                        THEN fws.conameorcik::INTEGER
                END AS VARCHAR
            ),
            fws.conameorcik) AS conameorcik,
            --nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik, -- noqa: LT05
            --ISNULL( convert(varchar,try_cast(conameorcik as bigint)), conameorcik) conameorcik, -- noqa: LT05
            s.st_name AS status,
            COALESCE(
                LEAD(fws.effectivedate) OVER (
                    PARTITION BY fws.symbol
                    ORDER BY fws.effectivedate
                ),
                --date('9999-12-31')
                TO_DATE('9999-12-31', 'yyyyMMdd')
            ) AS enddate
        FROM (
            SELECT
                TO_TIMESTAMP(
                    SUBSTRING(value, 1, 15), 'YYYYMMDD-HH24MISS'
                ) AS effectivedate,
                --        convert(date,convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112)) AS effectivedate, -- noqa: LT05
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name, -- noqa: RF04
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                TO_DATE(SUBSTRING(value, 133, 8), 'yyyyMMdd') AS firsttrade,
                --        convert(date,substring(value, 133, 8), 112) AS firsttrade, -- noqa: LT05
                TO_DATE(
                    SUBSTRING(value, 141, 8), 'yyyyMMdd'
                ) AS firsttradeonexchange,
                --        convert(date,substring(value, 141, 8), 112) AS firsttradeonexchange, -- noqa: LT05
                CAST(SUBSTRING(value, 149, 12) AS FLOAT) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('finwire') }}
            --FROM stg.FinWire
            WHERE rectype = 'SEC'
        ) AS fws
            INNER JOIN {{ source('tpcdi', 'StatusType') }} AS s
                ON s.st_id = fws.status
    ) AS fws
        INNER JOIN (
            SELECT
                sk_companyid,
                name AS conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('dimcompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                --cast(companyid as string) conameorcik,
                CAST(companyid AS VARCHAR) AS conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('dimcompany') }}
        ) AS dc
            ON
                fws.conameorcik = dc.conameorcik
                AND fws.effectivedate < dc.enddate
                AND fws.enddate > dc.effectivedate
) AS fws
--WHERE effectivedate != enddate
WHERE effectivedate != enddate
