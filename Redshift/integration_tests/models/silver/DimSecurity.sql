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
    CASE WHEN enddate = '9999-12-31'::DATE THEN 1 ELSE 0 END iscurrent,
    1 batchid,
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
        --if(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate,
        CASE
            WHEN
                fws.effectivedate < dc.effectivedate
                THEN dc.effectivedate
            ELSE fws.effectivedate
        END effectivedate,
        --if(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
        CASE
            WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate
        END enddate
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
                        conameorcik SIMILAR TO '[0-9]+(.[0-9][0-9])?'
                        THEN conameorcik::INTEGER
                    ELSE NULL
                END AS VARCHAR
            ),
            conameorcik) conameorcik,
            --nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik,
            --ISNULL( convert(varchar,try_cast(conameorcik as bigint)), conameorcik) conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(effectivedate) OVER (
                    PARTITION BY symbol
                    ORDER BY effectivedate
                ),
                --date('9999-12-31')
                TO_DATE('9999-12-31', 'yyyyMMdd')
            ) enddate
        FROM (
            SELECT
                TO_TIMESTAMP(
                    SUBSTRING(value, 1, 15), 'YYYYMMDD-HH24MISS'
                ) AS effectivedate,
                --        convert(date,convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112)) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name,
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                TO_DATE(SUBSTRING(value, 133, 8), 'yyyyMMdd') AS firsttrade,
                --        convert(date,substring(value, 133, 8), 112) AS firsttrade,
                TO_DATE(
                    SUBSTRING(value, 141, 8), 'yyyyMMdd'
                ) AS firsttradeonexchange,
                --        convert(date,substring(value, 141, 8), 112) AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS FLOAT) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('finwire') }}
            --FROM stg.FinWire
            WHERE rectype = 'SEC'
        ) fws
            JOIN {{ source('tpcdi', 'StatusType') }} s
                ON s.st_id = fws.status
    ) fws
        JOIN (
            SELECT
                sk_companyid,
                name conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('dimcompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                --cast(companyid as string) conameorcik,
                CAST(companyid AS VARCHAR) conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('dimcompany') }}
        ) dc
            ON
                fws.conameorcik = dc.conameorcik
                AND fws.effectivedate < dc.enddate
                AND fws.enddate > dc.effectivedate
) fws
--WHERE effectivedate != enddate
WHERE effectivedate != enddate
