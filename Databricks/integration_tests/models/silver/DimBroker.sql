{{ config(
  materialized='table'
) }}

SELECT

    CAST(employeeid AS BIGINT) brokerid,
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
        SELECT MIN(TO_DATE(datevalue)) AS effectivedate
        FROM {{ source('tpcdi', 'DimDate') }}
    ) effectivedate,
    DATE('9999-12-31') enddate,
    BIGINT(
        CONCAT(DATE_FORMAT(enddate, 'yyyyMMdd'), CAST(brokerid AS STRING))
    ) AS sk_brokerid
FROM {{ source('tpcdi', 'HR') }}
WHERE employeejobcode = 314
