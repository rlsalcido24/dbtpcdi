{{
    config(
        materialized = 'table'
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
    true AS iscurrent,
    1 AS batchid,
    (
        SELECT MIN(DATE(datevalue)) AS effectivedate
        FROM
            {{ source(var('benchmark'), 'DimDate') }} AS effectivedate,
            (SELECT DATE('9999-12-31'))
    ) AS enddate,
    (SELECT CONCAT(CAST(employeeid AS BIGINT), '-', DATE('9999-12-31')))
        AS sk_brokerid
FROM

    {{ source(var('benchmark'), 'HR') }}

WHERE
    employeejobcode = '314'
