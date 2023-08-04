{{
    config(
        materialized = 'table'
    )
}}
SELECT *
FROM (
    SELECT
        * EXCEPT (conameorcik),

        COALESCE(
            SAFE_CAST(CASE
                WHEN
                    CHAR_LENGTH(CAST(conameorcik AS STRING)) <= 10
                    THEN SAFE_CAST(conameorcik AS INT64)
                ELSE
                    NULL
            END AS STRING)
            ,
            SAFE_CAST(CASE
                WHEN
                    CHAR_LENGTH(CAST(conameorcik AS STRING)) > 10
                    OR SAFE_CAST(conameorcik AS INT64) IS NULL
                    THEN conameorcik
                ELSE
                    NULL
            END
            AS STRING)
        ) AS cik
    FROM (
        SELECT
            PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S', SUBSTRING(value, 1, 15)) AS pts,
            CAST(SUBSTRING(value, 19, 4) AS INT) AS fi_year,
            CAST(SUBSTRING(value, 23, 1) AS INT) AS fi_qtr,
            PARSE_DATE('%E4Y%m%d', SUBSTRING(value, 24, 8))
                AS fi_qtr_start_date,
            CAST(SUBSTRING(value, 40, 17) AS FLOAT64) AS fi_revenue,
            CAST(SUBSTRING(value, 57, 17) AS FLOAT64) AS fi_net_earn,
            CAST(SUBSTRING(value, 74, 12) AS FLOAT64) AS fi_basic_eps,
            CAST(SUBSTRING(value, 86, 12) AS FLOAT64) AS fi_dilut_eps,
            CAST(SUBSTRING(value, 98, 12) AS FLOAT64) AS fi_margin,
            CAST(SUBSTRING(value, 110, 17) AS FLOAT64) AS fi_inventory,
            CAST(SUBSTRING(value, 127, 17) AS FLOAT64) AS fi_assets,
            CAST(SUBSTRING(value, 144, 17) AS FLOAT64) AS fi_liability,
            CAST(SUBSTRING(value, 161, 13) AS BIGINT) AS fi_out_basic,
            CAST(SUBSTRING(value, 174, 13) AS BIGINT) AS fi_out_dilut,
            TRIM(SUBSTRING(value, 187, 60)) AS conameorcik
        FROM
            {{ ref('FinWire') }}
        WHERE rectype = "FIN"
    ) f
) f

JOIN (
    SELECT
        sk_companyid,
        name conameorcik,
        effectivedate,
        enddate
    FROM
        {{ ref('DimCompany') }}
    UNION ALL
    SELECT
        sk_companyid,
        CAST(companyid AS STRING) conameorcik,
        effectivedate,
        enddate
    FROM
        {{ ref('DimCompany') }}
) dc
    ON
        f.cik = dc.conameorcik
        AND DATE(pts) >= dc.effectivedate
        AND DATE(pts) < dc.enddate
