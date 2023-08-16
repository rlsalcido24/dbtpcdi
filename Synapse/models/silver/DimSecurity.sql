{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(symbol)'
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
    CASE WHEN enddate = CAST('9999-12-31' AS DATE) THEN 1 ELSE 0 END
        AS iscurrent,
    1 AS batchid,
    effectivedate,
    CONCAT(exchangeid, '-', effectivedate) AS sk_securityid,
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
            ISNULL(
                CAST(TRY_CAST(fws.conameorcik AS BIGINT) AS VARCHAR),
                fws.conameorcik
            ) AS conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(fws.effectivedate) OVER (
                    PARTITION BY fws.symbol
                    ORDER BY fws.effectivedate
                ),
                CAST('9999-12-31' AS DATE)
            ) AS enddate
        FROM (
            SELECT
                CAST(
                    CONVERT( --noqa: CV11
                        DATETIME2,
                        SUBSTRING(value, 1, 8)
                        + ' '
                        + SUBSTRING(value, 10, 2)
                        + ':'
                        + SUBSTRING(value, 12, 2)
                        + ':'
                        + SUBSTRING(value, 14, 2),
                        112
                    )
                    AS DATE
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name,
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                CONVERT( --noqa: CV11
                    DATE, SUBSTRING(value, 133, 8), 112
                ) AS firsttrade,
                CONVERT(DATE, SUBSTRING(value, 141, 8), 112) --noqa: CV11
                    AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS DECIMAL(10, 2)) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('FinWire_SEC') }}
        ) AS fws
            INNER JOIN {{ ref('StatusType') }} AS s
                ON s.st_id = fws.status
    ) AS fws
        INNER JOIN (
            SELECT
                sk_companyid,
                name AS conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                CAST(companyid AS VARCHAR) AS conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
        ) AS dc
            ON
                fws.conameorcik = dc.conameorcik
                AND fws.effectivedate < dc.enddate
                AND fws.enddate > dc.effectivedate
) AS fws
WHERE effectivedate != enddate
