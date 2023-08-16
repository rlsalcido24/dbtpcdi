{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}


SELECT
    CAST(employeeid AS BIGINT) AS brokerid,
    CAST(managerid AS BIGINT) AS managerid,
    employeefirstname AS firstname,
    employeelastname AS lastname,
    employeemi AS middleinitial,
    employeebranch AS branch,
    employeeoffice AS office,
    employeephone AS phone,
    1 AS iscurrent,
    1 AS batchid,
    (
        SELECT MIN(CAST(datevalue AS DATE)) AS effectivedate
        FROM {{ ref('DimDate') }}
    ) AS effectivedate,
    CAST('9999-12-31' AS DATE) AS enddate,
    CONCAT(CAST(employeeid AS BIGINT), '-', CAST('9999-12-31' AS DATE))
        AS sk_brokerid
FROM {{ ref('HR') }}
WHERE employeejobcode = 314
