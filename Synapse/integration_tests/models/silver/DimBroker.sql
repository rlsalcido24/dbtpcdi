{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}

        
SELECT
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  1 iscurrent,
  1 batchid,
  (SELECT min(cast(datevalue as date)) as effectivedate FROM {{ ref('DimDate') }}) effectivedate,
  cast('9999-12-31' as date) enddate,
  concat(cast(employeeid as BIGINT), '-', cast('9999-12-31' as date)) as sk_brokerid
FROM  {{ ref('HR') }}
WHERE employeejobcode = 314