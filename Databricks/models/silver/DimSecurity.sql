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
    IF(enddate = DATE('9999-12-31'), true, false) AS iscurrent,
    1 AS batchid,
    effectivedate,
    BIGINT(
        CONCAT(
            DATE_FORMAT(effectivedate, 'yyyyMMdd'), CAST(exchangeid AS STRING)
        )
    ) AS sk_securityid,
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
        IF(
            fws.effectivedate < dc.effectivedate,
            dc.effectivedate,
            fws.effectivedate
        ) AS effectivedate,
        IF(fws.enddate > dc.enddate, dc.enddate, fws.enddate) AS enddate
    FROM (
        SELECT
            fws.* EXCEPT (status, conameorcik),
            COALESCE(
                STRING(TRY_CAST(fws.conameorcik AS BIGINT)), fws.conameorcik
            ) AS conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(fws.effectivedate) OVER (
                    PARTITION BY fws.symbol
                    ORDER BY fws.effectivedate
                ),
                DATE('9999-12-31')
            ) AS enddate
        FROM (
            SELECT
                DATE(
                    TO_TIMESTAMP(SUBSTRING(value, 1, 15), 'yyyyMMdd-HHmmss')
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name, -- noqa:RF04
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                TO_DATE(SUBSTRING(value, 133, 8), 'yyyyMMdd') AS firsttrade,
                TO_DATE(
                    SUBSTRING(value, 141, 8), 'yyyyMMdd'
                ) AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS DOUBLE) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik
            FROM {{ ref('FinWire') }}
            WHERE rectype = 'SEC'
        ) AS fws
            INNER JOIN {{ source('tpcdi', 'StatusType') }} AS s
                ON s.st_id = fws.status
    ) AS fws
        INNER JOIN (
            SELECT
                sk_companyid,
                name AS conameorcik, -- noqa:RF04
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                CAST(companyid AS STRING) AS conameorcik,
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
