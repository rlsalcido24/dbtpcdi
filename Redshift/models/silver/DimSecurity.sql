{{
    config(
        materialized = 'table'
    )
}}

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
    CASE
        WHEN enddate = CAST('9999-12-31' AS DATE) THEN 1 ELSE 0
    END AS iscurrent,
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
        CASE
            WHEN
                fws.effectivedate < dc.effectivedate
                THEN dc.effectivedate
            ELSE fws.effectivedate
        END AS effectivedate,
        CASE
            WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate
        END AS enddate
    FROM (
        SELECT
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
                CAST(
                    CASE
                        WHEN
                            fws.conameorcik SIMILAR TO '[0-9]+(.[0-9][0-9])?'
                            THEN fws.conameorcik
                    END AS INTEGER
                ) AS VARCHAR
            ),
            fws.conameorcik) AS conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(fws.effectivedate) OVER (
                    PARTITION BY fws.symbol
                    ORDER BY fws.effectivedate
                ),
                TO_DATE('9999-12-31', 'yyyyMMdd')
            ) AS enddate
        FROM (
            SELECT
                TO_TIMESTAMP(
                    SUBSTRING(value, 1, 15), 'YYYYMMDD-HH24MISS'
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name, -- noqa: RF04
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                TO_DATE(SUBSTRING(value, 133, 8), 'yyyyMMdd') AS firsttrade,
                TO_DATE(
                    SUBSTRING(value, 141, 8), 'yyyyMMdd'
                ) AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS FLOAT) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('finwire') }}
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
WHERE effectivedate != enddate
