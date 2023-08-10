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
    IF(enddate = DATE('9999-12-31'), TRUE, FALSE) iscurrent,
    1 batchid,
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
        ) effectivedate,
        IF(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
    FROM (
        SELECT
            fws.* EXCEPT (status, conameorcik),
            COALESCE(
                STRING(TRY_CAST(conameorcik AS BIGINT)), conameorcik
            ) conameorcik,
            s.st_name AS status,
            COALESCE(
                LEAD(effectivedate) OVER (
                    PARTITION BY symbol
                    ORDER BY effectivedate
                ),
                DATE('9999-12-31')
            ) enddate
        FROM (
            SELECT
                DATE(
                    TO_TIMESTAMP(SUBSTRING(value, 1, 15), 'yyyyMMdd-HHmmss')
                ) AS effectivedate,
                TRIM(SUBSTRING(value, 19, 15)) AS symbol,
                TRIM(SUBSTRING(value, 34, 6)) AS issue,
                TRIM(SUBSTRING(value, 40, 4)) AS status,
                TRIM(SUBSTRING(value, 44, 70)) AS name,
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
            FROM {{ ref('DimCompany') }}
            UNION ALL
            SELECT
                sk_companyid,
                CAST(companyid AS STRING) conameorcik,
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
