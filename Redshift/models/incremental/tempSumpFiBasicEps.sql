{{
    config(
        materialized = 'table'
    )
}}

SELECT
    f.sk_companyid,
    f.fi_qtr_start_date,
    SUM(f.fi_basic_eps)
        OVER (
            PARTITION BY c.companyid
            ORDER BY f.fi_qtr_start_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    - f.fi_basic_eps AS sum_fi_basic_eps
FROM {{ ref('financial') }} AS f
    INNER JOIN {{ ref('dimcompany') }} AS c
        ON f.sk_companyid = c.sk_companyid
