{{
    config(
        materialized = 'table'
    )
}}
SELECT
    f.sk_companyid,
    f.fi_qtr_start_date,
    SUM(fi.fi_basic_eps)
        OVER (
            PARTITION BY dc.companyid
            ORDER BY f.fi_qtr_start_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    - f.fi_basic_eps AS sum_fi_basic_eps
FROM {{ ref('Financial') }} AS f
    INNER JOIN {{ ref('DimCompany') }} AS dc
        ON f.sk_companyid = dc.sk_companyid
