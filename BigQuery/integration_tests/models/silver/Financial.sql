{{
    config(
        materialized = 'table'
    )
}}
SELECT
*
  FROM (
    SELECT
      * except(conameorcik),

      IFNULL(
        SAFE_CAST(CASE
        WHEN CHAR_LENGTH(CAST(conameorcik AS STRING)) <= 10 THEN SAFE_CAST(conameorcik  AS INT64)
        ELSE
            NULL
        END AS STRING)
        ,
      SAFE_CAST(CASE
        WHEN CHAR_LENGTH(CAST(conameorcik AS STRING)) > 10 OR SAFE_CAST(conameorcik AS INT64) IS NULL THEN conameorcik
        ELSE
            NULL
        END
        AS STRING)) AS CIK
    FROM (
      SELECT
        PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S',substring(value, 1, 15)) AS PTS,
        cast(substring(value, 19, 4) AS INT) AS fi_year,
        cast(substring(value, 23, 1) AS INT) AS fi_qtr,
        PARSE_DATE('%E4Y%m%d', substring(value, 24, 8)) as fi_qtr_start_date,
        cast(substring(value, 40, 17) AS FLOAT64) AS fi_revenue,
        cast(substring(value, 57, 17) AS FLOAT64) AS fi_net_earn,
        cast(substring(value, 74, 12) AS FLOAT64) AS fi_basic_eps,
        cast(substring(value, 86, 12) AS FLOAT64) AS fi_dilut_eps,
        cast(substring(value, 98, 12) AS FLOAT64) AS fi_margin,
        cast(substring(value, 110, 17) AS FLOAT64) AS fi_inventory,
        cast(substring(value, 127, 17) AS FLOAT64) AS fi_assets,
        cast(substring(value, 144, 17) AS FLOAT64) AS fi_liability,
        cast(substring(value, 161, 13) AS BIGINT) AS fi_out_basic,
        cast(substring(value, 174, 13) AS BIGINT) AS fi_out_dilut,
        trim(substring(value, 187, 60)) AS conameorcik
      FROM
       {{ ref('FinWire') }} WHERE rectype="FIN" ) f ) f

JOIN (
    SELECT
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM
   {{ ref('DimCompany') }}
    UNION ALL
    SELECT
      sk_companyid,
      CAST(companyid AS string) conameorcik,
      EffectiveDate,
      EndDate
    FROM
     {{ ref('DimCompany') }}) dc
  ON
    f.CIK = dc.conameorcik
    AND DATE(PTS) >= dc.effectivedate
    AND DATE(PTS) < dc.enddate