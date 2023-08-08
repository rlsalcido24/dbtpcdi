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
        IF(
            fws.effectivedate < dc.effectivedate,
            dc.effectivedate,
            fws.effectivedate
        ) AS effectivedate,
        IF(fws.enddate > dc.enddate, dc.enddate, fws.enddate) AS enddate
    FROM (
        SELECT -- noqa: ST06
            fws.* EXCEPT (status, conameorcik),

            COALESCE(
                SAFE_CAST(CASE
                    WHEN
                        CHAR_LENGTH(CAST(conameorcik AS STRING)) <= 10
                        THEN SAFE_CAST(conameorcik AS INT64)
                END AS STRING)
                ,
                SAFE_CAST(CASE
                    WHEN
                        CHAR_LENGTH(CAST(conameorcik AS STRING)) > 10
                        OR SAFE_CAST(conameorcik AS INT64) IS NULL
                        THEN conameorcik
                END
                AS STRING)
            ) AS cik,
            s.st_name AS status,
            COALESCE(
                LEAD(effectivedate) OVER (
                    PARTITION BY symbol
                    ORDER BY effectivedate
                ),
                DATE('9999-12-31')
            ) AS enddate
        FROM (
            SELECT -- noqa: ST06
                DATE(
                    PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S', SUBSTRING(value, 1, 15))
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name,  -- noqa: RF04
                TRIM(SUBSTRING(value, 114, 6)) AS exchangeid,
                CAST(SUBSTRING(value, 120, 13) AS BIGINT) AS sharesoutstanding,
                PARSE_DATE(
                    '%E4Y%m%d', CAST(SUBSTRING(value, 133, 8) AS STRING)
                ) AS firsttrade,
                PARSE_DATE(
                    '%E4Y%m%d', CAST(SUBSTRING(value, 141, 8) AS STRING)
                ) AS firsttradeonexchange,
                CAST(SUBSTRING(value, 149, 12) AS FLOAT64) AS dividend,
                TRIM(SUBSTRING(value, 161, 60)) AS conameorcik


            FROM {{ ref('FinWire') }} WHERE rectype = 'SEC'

        ) AS fws
            INNER JOIN {{ source(var('benchmark'), 'StatusType') }} AS s
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
                CAST(companyid AS STRING) AS conameorcik,
                effectivedate,
                enddate
            FROM {{ ref('DimCompany') }}
        ) AS dc
            ON
                fws.cik = dc.conameorcik
                AND fws.effectivedate < dc.enddate
                AND fws.enddate > dc.effectivedate
) AS fws
WHERE effectivedate != enddate
