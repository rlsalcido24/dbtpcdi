{{
    config(
        materialized = 'table'
    )
}}

SELECT
  CAST(employeeid AS BIGINT) AS brokerid,
  CAST(managerid AS BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  TRUE iscurrent,
  1 batchid,
  (
  SELECT
    MIN(date(datevalue)) AS effectivedate
  FROM
    {{source(var('benchmark'), 'DimDate') }} effectivedate,
  (SELECT DATE('9999-12-31'))) enddate,
  (SELECT CONCAT(CAST(employeeid AS BIGINT), '-',  DATE('9999-12-31'))) AS sk_brokerid
FROM

  {{source(var('benchmark'), 'HR') }}
  
WHERE
  employeejobcode = "314"