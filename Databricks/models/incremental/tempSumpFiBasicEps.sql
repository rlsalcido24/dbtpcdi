{{
    config(
        materialized = 'table'
    )
}}
SELECT
    fi.sk_companyid,
    fi.fi_qtr_start_date,
    SUM(fi.fi_basic_eps)
        OVER (
            PARTITION BY dc.companyid
            ORDER BY fi.fi_qtr_start_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )
    - fi.fi_basic_eps AS sum_fi_basic_eps
FROM {{ ref('Financial') }} AS fi
    INNER JOIN {{ ref('DimCompany') }} AS dc
        ON fi.sk_companyid = dc.sk_companyid;
