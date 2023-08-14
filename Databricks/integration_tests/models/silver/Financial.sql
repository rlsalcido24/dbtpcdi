{{
    config(
        materialized = 'table'
    )
}}
SELECT
    dc.sk_companyid,
    f.fi_year,
    f.fi_qtr,
    f.fi_qtr_start_date,
    f.fi_revenue,
    f.fi_net_earn,
    f.fi_basic_eps,
    f.fi_dilut_eps,
    f.fi_margin,
    f.fi_inventory,
    f.fi_assets,
    f.fi_liability,
    f.fi_out_basic,
    f.fi_out_dilut
FROM (
    SELECT
        * EXCEPT (conameorcik),
        COALESCE(
            STRING(TRY_CAST(conameorcik AS BIGINT)), conameorcik
        ) AS conameorcik
    FROM (
        SELECT
            TO_TIMESTAMP(SUBSTRING(value, 1, 15), 'yyyyMMdd-HHmmss') AS pts,
            CAST(SUBSTRING(value, 19, 4) AS INT) AS fi_year,
            CAST(SUBSTRING(value, 23, 1) AS INT) AS fi_qtr,
            TO_DATE(SUBSTRING(value, 24, 8), 'yyyyMMdd') AS fi_qtr_start_date,
            --to_date(substring(value, 32, 8), 'yyyyMMdd') AS PostingDate,
            CAST(SUBSTRING(value, 40, 17) AS DOUBLE) AS fi_revenue,
            CAST(SUBSTRING(value, 57, 17) AS DOUBLE) AS fi_net_earn,
            CAST(SUBSTRING(value, 74, 12) AS DOUBLE) AS fi_basic_eps,
            CAST(SUBSTRING(value, 86, 12) AS DOUBLE) AS fi_dilut_eps,
            CAST(SUBSTRING(value, 98, 12) AS DOUBLE) AS fi_margin,
            CAST(SUBSTRING(value, 110, 17) AS DOUBLE) AS fi_inventory,
            CAST(SUBSTRING(value, 127, 17) AS DOUBLE) AS fi_assets,
            CAST(SUBSTRING(value, 144, 17) AS DOUBLE) AS fi_liability,
            CAST(SUBSTRING(value, 161, 13) AS BIGINT) AS fi_out_basic,
            CAST(SUBSTRING(value, 174, 13) AS BIGINT) AS fi_out_dilut,
            TRIM(SUBSTRING(value, 187, 60)) AS conameorcik
        FROM {{ ref('FinWire') }}
        WHERE rectype = 'FIN'
    ) AS f
) AS f
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
            f.conameorcik = dc.conameorcik
            AND DATE(f.pts) >= dc.effectivedate
            AND DATE(f.pts) < dc.enddate
