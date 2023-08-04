{{
    config(
        materialized = 'table'
    )
}}

SELECT
    sk_companyid,
    fi_year,
    fi_qtr,
    fi_qtr_start_date,
    fi_revenue,
    fi_net_earn,
    fi_basic_eps,
    fi_dilut_eps,
    fi_margin,
    fi_inventory,
    fi_assets,
    fi_liability,
    fi_out_basic,
    fi_out_dilut
FROM (
    SELECT
        * EXCEPT (conameorcik),
        COALESCE(
            STRING(TRY_CAST(conameorcik AS BIGINT)), conameorcik
        ) conameorcik
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
    ) f
) f
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
            f.conameorcik = dc.conameorcik
            AND DATE(pts) >= dc.effectivedate
            AND DATE(pts) < dc.enddate
