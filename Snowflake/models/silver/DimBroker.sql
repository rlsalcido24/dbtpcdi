{{ config(
  materialized='table'
) }}
SELECT
    CAST(employeeid AS BIGINT) AS brokerid,
    CAST(managerid AS BIGINT) AS managerid,
    employeefirstname AS firstname,
    employeelastname AS lastname,
    employeemi AS middleinitial,
    employeebranch AS branch,
    employeeoffice AS office,
    employeephone AS phone,
    true AS iscurrent,
    1 AS batchid,
    (
        SELECT MIN(TO_DATE(datevalue)) AS effectivedate
        FROM {{ source('tpcdi', 'DimDate') }}
    ) AS effectivedate,
    DATE('9999-12-31') AS enddate,
    CONCAT(brokerid, '-', enddate) AS sk_brokerid
FROM {{ source('tpcdi', 'HR') }}
WHERE employeejobcode = 314
