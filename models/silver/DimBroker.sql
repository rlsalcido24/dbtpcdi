{{ config(
  materialized='table'
) }}

SELECT
  md5(employeeid) as sk_brokerid,
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {{ source('tpcdi', 'DimDate') }}) effectivedate,
  date('9999-12-31') enddate
FROM  {{ source('tpcdi', 'HR') }}
WHERE employeejobcode = 314
