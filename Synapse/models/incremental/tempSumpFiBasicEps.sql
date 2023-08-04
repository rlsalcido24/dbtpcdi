{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(sk_companyid)'
    )
}}

        
SELECT
  f.sk_companyid,
  f.fi_qtr_start_date,
  sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
FROM {{ ref('Financial') }} f
JOIN {{ ref('DimCompany') }} c
ON f.sk_companyid=c.sk_companyid
