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
    CASE WHEN enddate = CONVERT(DATE, '9999-12-31') THEN 1 ELSE 0 END iscurrent,
    1 batchid,
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
        END effectivedate,
        CASE
            WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate
        END enddate
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
                CONVERT(VARCHAR, TRY_CAST(conameorcik AS BIGINT)), conameorcik
            ) conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(effectivedate) OVER (
                    PARTITION BY symbol
                    ORDER BY effectivedate
                ),
                CONVERT(DATE, '9999-12-31')
            ) enddate
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
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name,
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                CONVERT(DATE, SUBSTRING(value, 133, 8), 112) AS firsttrade,
                CONVERT(DATE, SUBSTRING(value, 141, 8), 112)
                    AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS DECIMAL(10, 2)) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('FinWire_SEC') }}
        ) fws
            JOIN {{ ref('StatusType') }} s
                ON s.st_id = fws.status
    ) fws
        JOIN (
            SELECT
                sk_companyid,
                name conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                CAST(companyid AS VARCHAR) conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
        ) dc
            ON
                fws.conameorcik = dc.conameorcik
                AND fws.effectivedate < dc.enddate
                AND fws.enddate > dc.effectivedate
) fws
WHERE effectivedate != enddate
